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

package org.apache.polaris.service.catalog.iceberg;

import jakarta.ws.rs.core.Response;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.polaris.core.config.FeatureConfiguration.OPTIMIZED_SIBLING_CHECK;
import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

public class IcebergAllowedLocationTest {
    private static final String namespace = "ns";
    private static final String catalog = "test-catalog";
    private String getTableName() {
        return "table_" + UUID.randomUUID();
    }

    @Test
    void testCreateTableWithPropertiesWriteMetadataPathAndNamespace(@TempDir Path tmpDir) {
        TestServices services = getTestServices();

        createCatalog(services, Map.of(), getBaseLocation(tmpDir));

        // create a namespace outside of catalog allowed locations
        createNamespace(services, "file:///tmp/ns1");

        // create a table outside of catalog allowed locations
        CreateTableRequest createTableRequest =
                CreateTableRequest.builder()
                        .withName(getTableName())
                        .withSchema(SCHEMA)
                        .build();

        try (Response createResponse =
                     services
                             .restApi()
                             .createTable(
                                     catalog,
                                     namespace,
                                     createTableRequest,
                                     null,
                                     services.realmContext(),
                                     services.securityContext())) {
            assertThat(createResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        }
    }

    private static TestServices getTestServices() {
        Map<String, Object> strictServicesWithOptimizedOverlapCheck =
                Map.of(
                        "ALLOW_TABLE_LOCATION_OVERLAP",
                        "true",
                        "ALLOW_INSECURE_STORAGE_TYPES",
                        "true",
                        "SUPPORTED_CATALOG_STORAGE_TYPES",
                        List.of("FILE"),
                        OPTIMIZED_SIBLING_CHECK.key(),
                        "true");
        TestServices services = TestServices.builder().config(strictServicesWithOptimizedOverlapCheck).build();
        return services;
    }

    private static @NotNull String getBaseLocation(Path tmpDir) {
        String baseLocation = tmpDir.toAbsolutePath().toUri().toString();
        if (baseLocation.endsWith("/")) {
            baseLocation = baseLocation.substring(0, baseLocation.length() - 1);
        }
        return baseLocation;
    }


    private void createCatalog(
            TestServices services, Map<String, String> catalogConfig, String catalogLocation) {
        CatalogProperties.Builder propertiesBuilder =
                CatalogProperties.builder()
                        .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, catalog))
                        .putAll(catalogConfig);

        StorageConfigInfo config =
                FileStorageConfigInfo.builder()
                        .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                        .build();
        Catalog catalogObject =
                new Catalog(
                        Catalog.TypeEnum.INTERNAL,
                        catalog,
                        propertiesBuilder.build(),
                        1725487592064L,
                        1725487592064L,
                        1,
                        config);
        try (Response response =
                     services
                             .catalogsApi()
                             .createCatalog(
                                     new CreateCatalogRequest(catalogObject),
                                     services.realmContext(),
                                     services.securityContext())) {
            assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
        }

    }

    private void createNamespace(TestServices services, String location) {
        Map<String, String> properties = new HashMap<>();
        properties.put("location", location);
        CreateNamespaceRequest createNamespaceRequest =
                CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).setProperties(properties).build();
        try (Response response =
                     services
                             .restApi()
                             .createNamespace(
                                     catalog,
                                     createNamespaceRequest,
                                     services.realmContext(),
                                     services.securityContext())) {
            assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        }
    }
}
