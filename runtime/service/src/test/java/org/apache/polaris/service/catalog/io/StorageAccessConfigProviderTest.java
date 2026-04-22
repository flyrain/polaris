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
package org.apache.polaris.service.catalog.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageCredentialsVendor;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Regression tests for {@link StorageAccessConfigProvider}'s behavior when {@link
 * FeatureConfiguration#SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION} is enabled.
 *
 * <p>The skip path must preserve non-credential routing properties (custom S3 endpoint, path-style
 * access) so that clients of S3-compatible backends do not silently fall back to AWS defaults. At
 * the same time, a vanilla AWS catalog with no overrides must still produce an empty config — the
 * behavior expected prior to the fix.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StorageAccessConfigProviderTest {

  private static final TableIdentifier TABLE =
      TableIdentifier.of(Namespace.of("ns"), "tbl");
  private static final Set<String> LOCATIONS = Set.of("s3://bucket/ns/tbl/");

  @Mock private StorageCredentialCache storageCredentialCache;
  @Mock private StorageCredentialsVendor storageCredentialsVendor;
  @Mock private RealmConfig realmConfig;
  @Mock private PolarisPrincipal polarisPrincipal;
  @Mock private RealmContext realmContext;

  private StorageAccessConfigProvider provider;

  @BeforeEach
  void setUp() {
    when(storageCredentialsVendor.getRealmConfig()).thenReturn(realmConfig);
    when(realmConfig.getConfig(eq(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION)))
        .thenReturn(true);
    provider =
        new StorageAccessConfigProvider(
            storageCredentialCache, storageCredentialsVendor, polarisPrincipal, realmContext);
  }

  @Test
  void skipSubscoping_vanillaAwsConfig_returnsEmptyAccessConfig() {
    AwsStorageConfigurationInfo awsConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .addAllowedLocation("s3://bucket/")
            .region("us-east-1")
            .build();

    StorageAccessConfig result = callProvider(awsConfig);

    assertThat(result.credentials()).isEmpty();
    assertThat(result.extraProperties()).isEmpty();
    assertThat(result.internalProperties()).isEmpty();
  }

  @Test
  void skipSubscoping_s3CompatibleEndpoint_preservesEndpointAndPathStyle() {
    AwsStorageConfigurationInfo awsConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .addAllowedLocation("s3://bucket/")
            .region("us-east-1")
            .endpoint("https://flashblade.example.com")
            .pathStyleAccess(true)
            .stsUnavailable(true)
            .build();

    StorageAccessConfig result = callProvider(awsConfig);

    assertThat(result.credentials()).isEmpty();
    assertThat(result.extraProperties())
        .containsEntry(
            StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), "https://flashblade.example.com")
        .containsEntry(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), "true")
        .doesNotContainKey(StorageAccessProperty.CLIENT_REGION.getPropertyName())
        .doesNotContainKey(StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT.getPropertyName());
    // With only a public endpoint set, internal falls back to the same URI.
    assertThat(result.internalProperties())
        .containsEntry(
            StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), "https://flashblade.example.com");
  }

  @Test
  void skipSubscoping_separateInternalEndpoint_isVendedAsInternalProperty() {
    AwsStorageConfigurationInfo awsConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .addAllowedLocation("s3://bucket/")
            .region("us-east-1")
            .endpoint("https://public.example.com")
            .endpointInternal("https://internal.example.com")
            .build();

    StorageAccessConfig result = callProvider(awsConfig);

    assertThat(result.extraProperties())
        .containsEntry(
            StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), "https://public.example.com");
    assertThat(result.internalProperties())
        .containsEntry(
            StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), "https://internal.example.com");
  }

  @Test
  void skipSubscoping_pathStyleOnlyWhenTrue() {
    AwsStorageConfigurationInfo awsConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .addAllowedLocation("s3://bucket/")
            .region("us-east-1")
            .endpoint("https://flashblade.example.com")
            .pathStyleAccess(false)
            .build();

    StorageAccessConfig result = callProvider(awsConfig);

    assertThat(result.extraProperties())
        .doesNotContainKey(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName());
  }

  private StorageAccessConfig callProvider(AwsStorageConfigurationInfo awsConfig) {
    PolarisEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setInternalProperties(
                Map.of(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                    awsConfig.serialize()))
            .build();
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(new ResolvedPolarisEntity(catalogEntity, List.of(), List.of())));
    return provider.getStorageAccessConfig(
        TABLE, LOCATIONS, Set.of(PolarisStorageActions.READ), Optional.empty(), resolvedPath);
  }
}
