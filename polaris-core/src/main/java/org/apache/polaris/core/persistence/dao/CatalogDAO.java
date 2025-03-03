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
package org.apache.polaris.core.persistence.dao;

import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;

public interface CatalogDAO {
  /**
   * Create a new catalog. This not only creates the new catalog entity but also the initial admin
   * role required to admin this catalog. If inline storage integration property is provided, create
   * a storage integration.
   *
   * @param callCtx call context
   * @param catalog the catalog entity to create
   * @param principalRoles once the catalog has been created, list of principal roles to grant its
   *     catalog_admin role to. If no principal role is specified, we will grant the catalog_admin
   *     role of the newly created catalog to the service admin role.
   * @return if success, the catalog which was created and its admin role.
   */
  @Nonnull
  PolarisMetaStoreManager.CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles);

  /**
   * todo call metaStoreManager.dropEntityIfExists( getCurrentPolarisContext(), null, entity,
   * Map.of(), cleanup); in FDB
   */
  @Nonnull
  PolarisMetaStoreManager.DropEntityResult dropCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore catalogToDrop,
      boolean cleanup);
}
