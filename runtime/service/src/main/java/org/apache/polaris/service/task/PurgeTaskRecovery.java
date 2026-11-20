/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.task;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Claims durable purge tasks for both immediate execution and recovery after a lost worker. */
final class PurgeTaskRecovery implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PurgeTaskRecovery.class);
  // The first execution and up to three recovery rounds. Exhausted records remain in the metastore.
  static final int MAX_ROUNDS = 4;
  private static final int PAGE_SIZE = 32;
  private final Clock clock;
  private final MetaStoreManagerFactory stores;
  private final ScheduledExecutorService scanner =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("polaris-purge-recovery").factory());
  private final ScheduledExecutorService heartbeats =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("polaris-purge-heartbeats").factory());
  private @Nullable ScheduledFuture<?> scanTask;
  private final Map<String, String> cursors = new HashMap<>();

  PurgeTaskRecovery(Clock clock, MetaStoreManagerFactory stores) {
    this.clock = clock;
    this.stores = stores;
  }

  @FunctionalInterface
  interface RecoveredTask {
    CompletionStage<Void> execute(PolarisBaseEntity task, CallContext context, Lease lease);
  }

  void start(List<String> realms, RealmConfigurationSource config, RecoveredTask execute) {
    scanTask =
        scanner.scheduleWithFixedDelay(
            () -> {
              for (String realm : realms) {
                try {
                  RealmContext realmContext = () -> realm;
                  var context =
                      new PolarisCallContext(
                          realmContext, stores.getOrCreateSession(realmContext), config);
                  scan(context, execute);
                } catch (RuntimeException e) {
                  LOGGER.warn("Could not recover purge tasks for realm {}", realm, e);
                }
              }
            },
            0,
            30,
            TimeUnit.SECONDS);
  }

  void scan(CallContext context, RecoveredTask execute) {
    String realm = context.getRealmContext().getRealmIdentifier();
    long timeout = timeout(context);
    var manager = stores.getOrCreateMetaStoreManager(context.getRealmContext());
    // loadTasks() leases every available row. Filter out exhausted and legacy rows before
    // pagination, then use the same versioned claim as the request that submitted the purge.
    var page =
        manager.listFullEntities(
            context.getPolarisCallContext(),
            null,
            PolarisEntityType.TASK,
            PolarisEntitySubType.NULL_SUBTYPE,
            task -> eligible(task, timeout),
            PageToken.build(cursors.get(realm), PAGE_SIZE, PAGE_SIZE, () -> true));
    cursors.put(realm, page.encodedResponseToken());
    for (var task : page.items()) {
      try {
        runIfAvailable(task, context, lease -> execute.execute(task, context, lease));
      } catch (RuntimeException e) {
        LOGGER.warn("Could not recover purge task {} in realm {}", task.getId(), realm, e);
      }
    }
  }

  private static long timeout(CallContext context) {
    var setting = FeatureConfiguration.POLARIS_TASK_TIMEOUT_MILLIS;
    long configured = context.getRealmConfig().getConfig(setting);
    return configured > 0 ? configured : setting.defaultValue();
  }

  private boolean eligible(PolarisBaseEntity task, long timeout) {
    if (!task.getInternalPropertiesAsMap().containsKey(TaskUtils.PURGE_IDENTITY)) {
      return false; // Older rows have no trusted storage session identity.
    }
    var state = PolarisObjectMapperUtil.parseTaskState(task);
    return state != null
        && state.getAttemptCount() < MAX_ROUNDS
        && (state.getExecutor() == null
            || clock.millis() - state.getLastAttemptStartTime() > timeout);
  }

  void runIfAvailable(
      PolarisBaseEntity task, CallContext context, Function<Lease, CompletionStage<Void>> execute) {
    var lease = claim(task, context);
    if (lease == null) {
      return;
    }
    try {
      execute.apply(lease).whenComplete((unused, failure) -> lease.release(failure));
    } catch (RuntimeException e) {
      lease.release(e);
      throw e;
    }
  }

  @Nullable Lease claim(PolarisBaseEntity task, CallContext context) {
    long timeout = timeout(context);
    if (!eligible(task, timeout)) {
      return null;
    }
    var manager = stores.getOrCreateMetaStoreManager(context.getRealmContext());
    var ctx = context.getPolarisCallContext();
    String parent = task.getInternalPropertiesAsMap().get(TaskUtils.PARENT_TASK);
    if (parent != null
        && manager
                .loadEntity(ctx, 0L, Long.parseLong(parent), PolarisEntityType.TASK)
                .getReturnStatus()
            != ReturnStatus.ENTITY_NOT_FOUND) {
      return null;
    }
    var state = PolarisObjectMapperUtil.parseTaskState(task);
    var claimed =
        new TaskEntity.Builder(TaskEntity.of(task))
            .withLastAttemptExecutorId(UUID.randomUUID().toString())
            .withAttemptCount(state.getAttemptCount() + 1)
            .withLastAttemptStartedTimestamp(clock.millis())
            .build();
    var result = manager.updateEntityPropertiesIfNotChanged(ctx, null, claimed);
    if (!result.isSuccess()) {
      if (result.getReturnStatus() == ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED
          || result.getReturnStatus() == ReturnStatus.ENTITY_NOT_FOUND) {
        return null;
      }
      throw new IllegalStateException("Unable to claim purge task: " + result.getReturnStatus());
    }
    return new Lease(manager, ctx, result.getEntity(), timeout, state.getAttemptCount() + 1);
  }

  final class Lease {
    private final PolarisMetaStoreManager manager;
    private final PolarisCallContext context;
    private final ScheduledFuture<?> heartbeat;
    private final int round;
    private final long taskId;
    private @Nullable PolarisBaseEntity claimed;

    Lease(
        PolarisMetaStoreManager manager,
        PolarisCallContext context,
        PolarisBaseEntity claimed,
        long timeout,
        int round) {
      this.manager = manager;
      this.context = context;
      this.claimed = claimed;
      this.taskId = claimed.getId();
      this.round = round;
      long period = Math.max(1L, timeout / 3);
      heartbeat =
          heartbeats.scheduleWithFixedDelay(this::renew, period, period, TimeUnit.MILLISECONDS);
    }

    void release(@Nullable Throwable failure) {
      heartbeat.cancel(false);
      if (failure != null && round == MAX_ROUNDS) {
        LOGGER.error(
            "Purge task {} exhausted its recovery attempts in realm {}",
            taskId,
            context.getRealmContext().getRealmIdentifier(),
            failure);
      }
    }

    synchronized boolean owns(PolarisBaseEntity task) {
      return claimed != null
          && PolarisObjectMapperUtil.parseTaskState(claimed)
              .getExecutor()
              .equals(PolarisObjectMapperUtil.parseTaskState(task).getExecutor());
    }

    synchronized void renew() {
      try {
        refresh();
      } catch (RuntimeException e) {
        LOGGER.warn("Could not renew purge task {}", taskId, e);
      }
    }

    private void refresh() {
      if (claimed == null) {
        return;
      }
      var updated =
          new TaskEntity.Builder(TaskEntity.of(claimed))
              .withLastAttemptStartedTimestamp(clock.millis())
              .build();
      var result = manager.updateEntityPropertiesIfNotChanged(context, null, updated);
      if (result.isSuccess()) {
        claimed = result.getEntity();
      } else if (result.getReturnStatus() == ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED
          || result.getReturnStatus() == ReturnStatus.ENTITY_NOT_FOUND) {
        claimed = null;
      } else {
        throw new IllegalStateException("Could not renew purge task: " + result.getReturnStatus());
      }
    }

    synchronized void complete() {
      // Verify the lease before retiring a row after a worker pause. Both immediate execution and
      // recovery acquire ownership by comparing the persisted entity version.
      refresh();
      if (claimed != null) {
        var result = manager.dropEntityIfExists(context, null, claimed, Map.of(), false);
        if (!result.isSuccess() && result.getReturnStatus() != ReturnStatus.ENTITY_NOT_FOUND) {
          throw new IllegalStateException(
              "Could not complete purge task: " + result.getReturnStatus());
        }
        claimed = null;
      }
    }
  }

  @Override
  public void close() {
    if (scanTask != null) {
      scanTask.cancel(false);
    }
    scanner.shutdownNow();
    heartbeats.shutdownNow();
  }
}
