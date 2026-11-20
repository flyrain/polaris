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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.junit.jupiter.api.Test;

class PurgeTaskRecoveryTest {
  private final TestServices services = TestServices.builder().build();
  private final PolarisCallContext context = services.newCallContext();
  private final Map<String, String> identity =
      TaskUtils.withPurgeIdentity(
          Map.of(), PolarisPrincipal.of("alice", AttributeMap.EMPTY, Set.of("writer")));
  private final long timeout =
      context.getRealmConfig().getConfig(FeatureConfiguration.POLARIS_TASK_TIMEOUT_MILLIS);

  private PurgeTaskRecovery workerAt(long millis) {
    return new PurgeTaskRecovery(
        Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC),
        services.metaStoreManagerFactory());
  }

  private PolarisBaseEntity persist(String name, Map<String, String> properties) {
    var task =
        new TaskEntity.Builder()
            .setName(name)
            .setId(services.metaStoreManager().generateNewEntityId(context).getId())
            .setCreateTimestamp(services.clock().millis())
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .setInternalProperties(properties)
            .build();
    return services.metaStoreManager().createEntityIfNotExists(context, null, task).getEntity();
  }

  @Test
  void reclaimAfterRestartAndRejectAStaleClaim() {
    var task = persist("purge", identity);
    try (var first = workerAt(1)) {
      first.runIfAvailable(task, context, lease -> new CompletableFuture<>());
    }
    try (var second = workerAt(timeout);
        var restarted = workerAt(timeout + 2)) {
      second.runIfAvailable(task, context, lease -> fail("A stale task version cannot be claimed"));
      second.scan(context, (t, c, lease) -> fail("A live lease cannot be taken over"));
      var calls = new AtomicInteger();
      restarted.scan(
          context,
          (t, c, lease) -> {
            var actor =
                PolarisObjectMapperUtil.deserialize(
                        t.getInternalPropertiesAsMap().get(TaskUtils.PURGE_IDENTITY),
                        TaskUtils.PurgeIdentity.class)
                    .principal();
            assertThat(actor.getName()).isEqualTo("alice");
            assertThat(actor.getRoles()).containsExactly("writer");
            calls.incrementAndGet();
            lease.complete();
            return CompletableFuture.completedFuture(null);
          });
      assertThat(calls.get()).isOne();
      assertThat(
              services
                  .metaStoreManager()
                  .loadEntity(context, 0L, task.getId(), PolarisEntityType.TASK)
                  .getReturnStatus())
          .isEqualTo(ReturnStatus.ENTITY_NOT_FOUND);
    }
  }

  @Test
  void recoveredBatchPurgesOnlyTheRemainingFiles() {
    String root = "s3://warehouse/table";
    String remaining = root + "/remaining";
    var table = TableIdentifier.of("ns", "table");
    var props = new HashMap<>(identity);
    props.put(
        PolarisTaskConstants.STORAGE_LOCATIONS, TaskUtils.encodeStorageLocations(Set.of(root)));
    var task =
        new TaskEntity.Builder()
            .setName("batch")
            .setCreateTimestamp(services.clock().millis())
            .setId(services.metaStoreManager().generateNewEntityId(context).getId())
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .withData(
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                    table,
                    List.of(root + "/already-gone", remaining),
                    BatchFileCleanupTaskHandler.BatchFileType.TABLE_METADATA))
            .setInternalProperties(props)
            .build();
    services.metaStoreManager().createEntityIfNotExists(context, null, task);
    var io = new InMemoryFileIO();
    io.addFile(remaining, new byte[] {1});
    var supplier = mock(TaskFileIOSupplier.class);
    when(supplier.apply(any(), any())).thenReturn(io);
    var executor =
        new TaskExecutorImpl(
            Runnable::run,
            null,
            services.clock(),
            services.metaStoreManagerFactory(),
            supplier,
            new RealmContextHolder(),
            services.polarisEventDispatcher(),
            services.eventMetadataFactory(),
            null,
            new PolarisPrincipalHolder(),
            services.principal(),
            mock(TaskHandlerConfiguration.class));
    try (var recovery = workerAt(1);
        var fileWorkers = Executors.newVirtualThreadPerTaskExecutor()) {
      executor.addTaskHandler(
          new BatchFileCleanupTaskHandler(supplier, fileWorkers, Duration.ofSeconds(10)));
      recovery.scan(context, executor::executeRecovered);
      assertThat(io.fileExists(remaining)).isFalse();
      assertThat(
              services
                  .metaStoreManager()
                  .loadEntity(context, 0L, task.getId(), PolarisEntityType.TASK)
                  .getReturnStatus())
          .isEqualTo(ReturnStatus.ENTITY_NOT_FOUND);
    } finally {
      executor.stopRecovery();
    }
  }

  @Test
  void exhaustedAndLegacyTasksStayPersistedWithoutStarvingNewTasks() {
    var task = persist("fails", identity);
    var old = persist("legacy", Map.of());
    var attempts = new AtomicInteger();
    for (int round = 0; round <= PurgeTaskRecovery.MAX_ROUNDS; round++) {
      try (var recovery = workerAt((timeout + 1) * round)) {
        recovery.scan(
            context,
            (t, c, lease) -> {
              assertThat(t.getId()).isEqualTo(task.getId());
              attempts.incrementAndGet();
              return CompletableFuture.failedFuture(
                  new IllegalStateException("storage unavailable"));
            });
      }
    }
    assertThat(attempts.get()).isEqualTo(PurgeTaskRecovery.MAX_ROUNDS);
    for (var retained : List.of(task, old)) {
      assertThat(
              services
                  .metaStoreManager()
                  .loadEntity(context, 0L, retained.getId(), PolarisEntityType.TASK)
                  .isSuccess())
          .isTrue();
    }
    var next = persist("next", identity);
    try (var worker = workerAt((timeout + 1) * (PurgeTaskRecovery.MAX_ROUNDS + 1))) {
      worker.scan(
          context,
          (t, c, lease) -> {
            assertThat(t.getId()).isEqualTo(next.getId());
            attempts.incrementAndGet();
            lease.complete();
            return CompletableFuture.completedFuture(null);
          });
    }
    assertThat(attempts.get()).isEqualTo(PurgeTaskRecovery.MAX_ROUNDS + 1);
  }

  @Test
  void staleWorkerCannotRetireANewerClaim() {
    var task = persist("purge", identity);
    try (var old = workerAt(1);
        var replacement = workerAt(timeout + 2)) {
      var originalLease = old.claim(task, context);
      var abandoned =
          services
              .metaStoreManager()
              .loadEntity(context, 0L, task.getId(), PolarisEntityType.TASK)
              .getEntity();
      var newLease = replacement.claim(abandoned, context);
      assertThat(newLease).isNotNull();
      originalLease.complete();
      var current =
          services.metaStoreManager().loadEntity(context, 0L, task.getId(), PolarisEntityType.TASK);
      assertThat(current.isSuccess()).isTrue();
      assertThat(newLease.owns(current.getEntity())).isTrue();
      newLease.complete();
    }
  }

  @Test
  void replayAfterChildrenWerePersistedDoesNotReadDeletedManifestLists() throws IOException {
    var failFirstDelete = new AtomicBoolean(true);
    String metadata = "v1.metadata.json";
    var io =
        new InMemoryFileIO() {
          @Override
          public void close() {
            // Each handler closes its FileIO. Keep the shared in-memory backing store across
            // attempts.
          }

          @Override
          public void deleteFile(String path) {
            if (path.equals(metadata) && failFirstDelete.getAndSet(false)) {
              throw new IllegalStateException("interrupted after persisting children");
            }
            super.deleteFile(path);
          }
        };
    var files = mock(TaskFileIOSupplier.class);
    when(files.apply(any(), any())).thenReturn(io);
    var handler =
        new TableCleanupTaskHandler(
            mock(TaskExecutor.class), services.clock(), services.metaStoreManagerFactory(), files);
    var manifest = TaskTestUtils.manifestFile(io, "manifest.avro", 100L, "data.parquet");
    var snapshot = TaskTestUtils.newSnapshot(io, "manifestList.avro", 1, 100L, 99L, manifest);
    TaskTestUtils.writeTableMetadata(io, metadata, snapshot);
    var parent =
        new TaskEntity.Builder()
            .setName("parent")
            .setId(200)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .setInternalProperties(identity)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE,
                        TableIdentifier.of("ns", "tbl"),
                        metadata)
                    .setName("tbl")
                    .setCatalogId(1)
                    .build())
            .build();
    assertThatThrownBy(() -> handler.handleTask(parent, context))
        .hasMessage("interrupted after persisting children");
    io.deleteFile(snapshot.manifestListLocation());
    handler.handleTask(parent, context);
    assertThat(TaskUtils.exists(metadata, io)).isFalse();
  }

  @Test
  void firstChildRemainsDiscoverableUntilParentRetires() {
    var parent = persist("parent", identity);
    var props = new HashMap<>(identity);
    props.put(TaskUtils.PARENT_TASK, Long.toString(parent.getId()));
    var child = persist(TableCleanupTaskHandler.firstChildName(parent.getId()), props);
    try (var worker = workerAt(1)) {
      worker.runIfAvailable(child, context, lease -> fail("Parent still needs its plan marker"));
      services.metaStoreManager().dropEntityIfExists(context, null, parent, Map.of(), false);
      worker.scan(
          context,
          (t, c, lease) -> {
            assertThat(t.getId()).isEqualTo(child.getId());
            lease.complete();
            return CompletableFuture.completedFuture(null);
          });
      assertThat(
              services
                  .metaStoreManager()
                  .loadEntity(context, 0L, child.getId(), PolarisEntityType.TASK)
                  .getReturnStatus())
          .isEqualTo(ReturnStatus.ENTITY_NOT_FOUND);
    }
  }
}
