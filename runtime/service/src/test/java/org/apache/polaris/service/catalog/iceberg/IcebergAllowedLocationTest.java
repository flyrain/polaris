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

import static org.apache.polaris.core.config.FeatureConfiguration.OPTIMIZED_SIBLING_CHECK;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY;
import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class IcebergAllowedLocationTest {
  private static final String VIEW_QUERY = "select * from ns.tbl";

  private static final String namespace = "ns";
  private static final String catalog = "test-catalog";

  private String getTableName() {
    return "table_" + UUID.randomUUID();
  }

  @Test
  void testCreateTableOutSideOfCatalogAllowedLocations(@TempDir Path tmpDir) {
    var services = getTestServices();

    var catalogLocation = tmpDir.resolve(catalog).toAbsolutePath().toUri().toString();
    var namespaceLocation = tmpDir.resolve(namespace).toAbsolutePath().toUri().toString();
    assertNotEquals(catalogLocation, namespaceLocation);

    createCatalog(services, Map.of(), catalogLocation, null);

    // create a namespace outside of catalog allowed locations
    createNamespace(services, namespaceLocation);

    // create a table under the namespace
    var createTableRequest =
        CreateTableRequest.builder().withName(getTableName()).withSchema(SCHEMA).build();

    assertThrows(
        ForbiddenException.class,
        () ->
            services
                .restApi()
                .createTable(
                    catalog,
                    namespace,
                    createTableRequest,
                    null,
                    services.realmContext(),
                    services.securityContext()));
  }

  @Test
  void testCreateTableInsideOfCatalogAllowedLocations(@TempDir Path tmpDir) {
    var services = getTestServices();

    var catalogLocation = tmpDir.resolve(catalog).toAbsolutePath().toUri().toString();
    var namespaceLocation = tmpDir.resolve(namespace).toAbsolutePath().toUri().toString();
    assertNotEquals(catalogLocation, namespaceLocation);

    // add the namespace location to the allowed locations of the catalog
    createCatalog(services, Map.of(), catalogLocation, List.of(namespaceLocation));

    createNamespace(services, namespaceLocation);

    // create a table under the namespace
    var createTableRequest =
        CreateTableRequest.builder().withName(getTableName()).withSchema(SCHEMA).build();

    var response =
        services
            .restApi()
            .createTable(
                catalog,
                namespace,
                createTableRequest,
                null,
                services.realmContext(),
                services.securityContext());

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
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
    TestServices services =
        TestServices.builder().config(strictServicesWithOptimizedOverlapCheck).build();
    return services;
  }

  @Test
  void testViewWithAllowedLocations(@TempDir Path tmpDir) {
    var viewName = "view";
    var services = getTestServices();
    var catalogLocation = tmpDir.resolve(catalog).toAbsolutePath().toUri().toString();
    createCatalog(services, Map.of(), catalogLocation, List.of(catalogLocation));
    var namespaceLocation = catalogLocation + "/" + namespace;
    createNamespace(services, namespaceLocation);

    // create a view with allowed locations
    String customAllowedLocation1 = Paths.get(namespaceLocation, "custom-location1").toString();
    String customAllowedLocation2 = Paths.get(namespaceLocation, "custom-location2").toString();

    var properties = new HashMap<String, String>();
    properties.put(USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, customAllowedLocation2);
    var viewVersion =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(System.currentTimeMillis())
            .schemaId(1)
            .defaultNamespace(Namespace.of(namespace))
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder().sql(VIEW_QUERY).dialect("spark").build())
            .build();
    CreateViewRequest createViewRequest =
        ImmutableCreateViewRequest.builder()
            .name(viewName)
            .schema(SCHEMA)
            .viewVersion(viewVersion)
            .location(customAllowedLocation1)
            .properties(properties)
            .build();
    var response =
        services
            .restApi()
            .createView(
                catalog,
                namespace,
                createViewRequest,
                services.realmContext(),
                services.securityContext());

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    // update the view with allowed locations
    String customAllowedLocation3 = Paths.get(namespaceLocation, "custom-location3").toString();

    Map<String, String> updatedProperties = new HashMap<>();
    updatedProperties.put(USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, customAllowedLocation3);

    UpdateTableRequest updateRequest =
        UpdateTableRequest.create(
            TableIdentifier.of(namespace, viewName),
            List.of(), // requirements
            List.of(new MetadataUpdate.SetProperties(updatedProperties)));

    var updateResponse =
        services
            .catalogAdapter()
            .newHandlerWrapper(services.securityContext(), catalog)
            .replaceView(TableIdentifier.of(namespace, viewName), updateRequest);
    assertEquals(
        updateResponse.metadata().properties().get(USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY),
        customAllowedLocation3);
  }

  @Test
  void testViewOutsideAllowedLocations(@TempDir Path tmpDir) {
    var services = getTestServices();

    var catalogBaseLocation = tmpDir.resolve(catalog).toAbsolutePath().toUri().toString();
    var namespaceLocation = catalogBaseLocation + "/" + namespace;

    createCatalog(services, Map.of(), catalogBaseLocation, List.of(catalogBaseLocation));
    createNamespace(services, namespaceLocation);

    var locationNotAllowed = Paths.get(tmpDir.toUri().toString()).toString();
    var locationAllowed = Paths.get(namespaceLocation, "custom-location").toString();

    // Test 1: Create a view with allowed location, and update it with a location not allowed
    var properties = new HashMap<String, String>();
    var viewVersion =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(System.currentTimeMillis())
            .schemaId(1)
            .defaultNamespace(Namespace.of(namespace))
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder().sql(VIEW_QUERY).dialect("spark").build())
            .build();

    CreateViewRequest createViewRequest =
        ImmutableCreateViewRequest.builder()
            .name("view")
            .schema(SCHEMA)
            .viewVersion(viewVersion)
            .location(locationAllowed)
            .properties(properties)
            .build();

    var response =
        services
            .restApi()
            .createView(
                catalog,
                namespace,
                createViewRequest,
                services.realmContext(),
                services.securityContext());
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    Map<String, String> updatedProperties = new HashMap<>();
    updatedProperties.put(USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, locationNotAllowed);

    UpdateTableRequest updateRequest =
            UpdateTableRequest.create(
                    TableIdentifier.of(namespace, "view"),
                    List.of(), // requirements
                    List.of(new MetadataUpdate.SetProperties(updatedProperties)));

    assertThatThrownBy(
            () ->
            services
                    .catalogAdapter()
                    .newHandlerWrapper(services.securityContext(), catalog)
                    .replaceView(TableIdentifier.of(namespace, "view"), updateRequest));

    // Test 2: Try to create a view with location not allowed
    var properties2 = new HashMap<String, String>();
    properties2.put(USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, locationNotAllowed);

    CreateViewRequest createViewRequestNotAllowed =
        ImmutableCreateViewRequest.builder()
            .name("view2")
            .schema(SCHEMA)
            .viewVersion(viewVersion)
            .location(locationNotAllowed)
            .properties(properties2)
            .build();

    assertThatThrownBy(
            () ->
                services
                    .restApi()
                    .createView(
                        catalog,
                        namespace,
                        createViewRequestNotAllowed,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid locations");

    // Test 3: Try to create a view with metadata location not allowed
    var properties3 = new HashMap<String, String>();
    properties3.put(USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, locationNotAllowed);

    CreateViewRequest createViewRequestMetadataNotAllowed =
        ImmutableCreateViewRequest.builder()
            .name("view3")
            .schema(SCHEMA)
            .viewVersion(viewVersion)
            .location(locationAllowed)
            .properties(properties3)
            .build();

    assertThatThrownBy(
            () ->
                services
                    .restApi()
                    .createView(
                        catalog,
                        namespace,
                        createViewRequestMetadataNotAllowed,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid locations");
  }

  private void createCatalog(
      TestServices services,
      Map<String, String> catalogConfig,
      String catalogLocation,
      List<String> allowedLocations) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, catalog))
            .putAll(catalogConfig);

    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(allowedLocations)
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
    if (location != null) {
      properties.put("location", location);
    }
    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of(namespace))
            .setProperties(properties)
            .build();
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
