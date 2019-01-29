/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.gcs;

import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.connect.gcs.format.avro.AvroFormat;
import io.confluent.connect.gcs.format.json.JsonFormat;
import io.confluent.connect.gcs.storage.GcsStorage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.*;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GcsSinkConnectorConfigTest extends GcsSinkConnectorTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testStorageClass() {
    // No real test case yet
    connectorConfig = new GcsSinkConnectorConfig(properties);
    assertEquals(
        GcsStorage.class,
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG)
    );
  }

  @Test
  public void testUndefinedURL() {
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    connectorConfig = new GcsSinkConnectorConfig(properties);
    assertNull(connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG));
  }

  @Test
  public void testRecommendedValues() {
    List<Object> expectedStorageClasses = Arrays.<Object>asList(GcsStorage.class);
    List<Object> expectedFormatClasses = Arrays.<Object>asList(
        AvroFormat.class,
        JsonFormat.class
    );
    List<Object> expectedPartitionerClasses = Arrays.<Object>asList(
        DefaultPartitioner.class,
        HourlyPartitioner.class,
        DailyPartitioner.class,
        TimeBasedPartitioner.class,
        FieldPartitioner.class
    );

    List<ConfigValue> values = GcsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      if (val.value() instanceof Class) {
        switch (val.name()) {
          case StorageCommonConfig.STORAGE_CLASS_CONFIG:
            assertEquals(expectedStorageClasses, val.recommendedValues());
            break;
          case GcsSinkConnectorConfig.FORMAT_CLASS_CONFIG:
            assertEquals(expectedFormatClasses, val.recommendedValues());
            break;
          case PartitionerConfig.PARTITIONER_CLASS_CONFIG:
            assertEquals(expectedPartitionerClasses, val.recommendedValues());
            break;
        }
      }
    }
  }

  @Test
  public void testAvroDataConfigSupported() {
    properties.put(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
    properties.put(AvroDataConfig.CONNECT_META_DATA_CONFIG, "false");
    connectorConfig = new GcsSinkConnectorConfig(properties);
    assertEquals(true, connectorConfig.get(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG));
    assertEquals(false, connectorConfig.get(AvroDataConfig.CONNECT_META_DATA_CONFIG));
  }

  @Test
  public void testVisibilityForPartitionerClassDependentConfigs() {
    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
    List<ConfigValue> values = GcsSinkConnectorConfig.getConfig().validate(properties);
    assertDefaultPartitionerVisibility(values);

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
    assertFieldPartitionerVisibility();

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DailyPartitioner.class.getName());
    values = GcsSinkConnectorConfig.getConfig().validate(properties);
    assertTimeBasedPartitionerVisibility(values);

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, HourlyPartitioner.class.getName());
    values = GcsSinkConnectorConfig.getConfig().validate(properties);
    assertTimeBasedPartitionerVisibility(values);

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        TimeBasedPartitioner.class.getName()
    );
    values = GcsSinkConnectorConfig.getConfig().validate(properties);
    assertNullPartitionerVisibility(values);

    Partitioner<?> klass = new Partitioner<FieldSchema>() {
      @Override
      public void configure(Map<String, Object> config) {}

      @Override
      public String encodePartition(SinkRecord sinkRecord) {
        return null;
      }

      @Override
      public String generatePartitionedPath(String topic, String encodedPartition) {
        return null;
      }

      @Override
      public List<FieldSchema> partitionFields() {
        return null;
      }
    };

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        klass.getClass().getName()
    );
    values = GcsSinkConnectorConfig.getConfig().validate(properties);
    assertNullPartitionerVisibility(values);
  }

  @Test
  @Ignore
  public void testConfigurableCredentialProvider() {
    // TODO Add support for Credential provider in GCS Connector if needed.
//    final String ACCESS_KEY_VALUE = "AKIAAAAAKKKKIIIIAAAA";
//    final String SECRET_KEY_VALUE = "WhoIsJohnGalt?";
//
//    properties.put(
//        GcsSinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
//        DummyAssertiveCredentialsProvider.class.getName()
//    );
//    String configPrefix = GcsSinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
//    properties.put(
//        configPrefix.concat(DummyAssertiveCredentialsProvider.ACCESS_KEY_NAME),
//        ACCESS_KEY_VALUE
//    );
//    properties.put(
//        configPrefix.concat(DummyAssertiveCredentialsProvider.SECRET_KEY_NAME),
//        SECRET_KEY_VALUE
//    );
//    properties.put(
//        configPrefix.concat(DummyAssertiveCredentialsProvider.CONFIGS_NUM_KEY_NAME),
//        "3"
//    );
//    connectorConfig = new GcsSinkConnectorConfig(properties);
//
//    AWSCredentialsProvider credentialsProvider = connectorConfig.getCredentialsProvider();
//
//    assertEquals(ACCESS_KEY_VALUE, credentialsProvider.getCredentials().getAWSAccessKeyId());
//    assertEquals(SECRET_KEY_VALUE, credentialsProvider.getCredentials().getAWSSecretKey());
  }

  @Test
  @Ignore
  public void testConfigurableCredentialProviderMissingConfigs() {
    // TODO Add support for Credential provider in GCS Connector if needed.
//    thrown.expect(ConfigException.class);
//    thrown.expectMessage("are mandatory configuration properties");
//
//    String configPrefix = GcsSinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
//    properties.put(
//        GcsSinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
//        DummyAssertiveCredentialsProvider.class.getName()
//    );
//    properties.put(
//        configPrefix.concat(DummyAssertiveCredentialsProvider.CONFIGS_NUM_KEY_NAME),
//        "2"
//    );
//
//    connectorConfig = new GcsSinkConnectorConfig(properties);
//    connectorConfig.getCredentialsProvider();
  }

  private void assertDefaultPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }
  }

  private void assertFieldPartitionerVisibility() {
    List<ConfigValue> values;
    values = GcsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
          assertTrue(val.visible());
          break;
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }
  }

  private void assertTimeBasedPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }

  private void assertNullPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }
  @Test(expected = ConfigException.class)
  public void testGcsPartRetriesNegative() {
    properties.put(GcsSinkConnectorConfig.GCS_PART_RETRIES_CONFIG, "-1");
    connectorConfig = new GcsSinkConnectorConfig(properties);
    connectorConfig.getInt(GcsSinkConnectorConfig.GCS_PART_RETRIES_CONFIG);
  }

  @Test(expected = ConfigException.class)
  public void testGcsRetryBackoffNegative() {
    properties.put(GcsSinkConnectorConfig.GCS_RETRY_BACKOFF_CONFIG, "-1");
    connectorConfig = new GcsSinkConnectorConfig(properties);
    connectorConfig.getLong(GcsSinkConnectorConfig.GCS_RETRY_BACKOFF_CONFIG);
  }
}

