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

import com.google.cloud.storage.Storage;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.gcs.format.avro.AvroFormat;
import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.schema.SchemaCompatibility;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GcsSinkConnectorTestBase extends StorageSinkTestBase {

  private static final Logger log = LoggerFactory.getLogger(GcsSinkConnectorTestBase.class);

  protected static final String GCS_TEST_URL = "http://127.0.0.1:8181";
  protected static final String GCS_TEST_BUCKET_NAME = "kafka-connect-gcs-test-cew213nd223";
  protected static final Time SYSTEM_TIME = new SystemTime();

  protected static GcsSinkConnectorConfig connectorConfig;
  protected String topicsDir;
  protected Map<String, Object> parsedConfig;
  protected SchemaCompatibility compatibility;

  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      log.info(
          "Starting test: {}.{}",
          description.getTestClass().getSimpleName(),
          description.getMethodName()
      );
    }
  };

  @Override
  protected Map<String, String> createProps() {
    url = GCS_TEST_URL;
    Map<String, String> props = super.createProps();
    props.put(StorageCommonConfig.STORAGE_CLASS_CONFIG, "io.confluent.connect.gcs.storage.GcsStorage");
    props.put(GcsSinkConnectorConfig.GCS_BUCKET_CONFIG, GCS_TEST_BUCKET_NAME);
    props.put(GcsSinkConnectorConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, PartitionerConfig.PARTITIONER_CLASS_DEFAULT.getName());
    props.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "int");
    props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd_'hour'=HH");
    props.put(PartitionerConfig.LOCALE_CONFIG, "en");
    props.put(PartitionerConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
    props.put(HiveConfig.HIVE_CONF_DIR_CONFIG, "America/Los_Angeles");
    return props;
  }

  //@Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    connectorConfig = PowerMockito.spy(new GcsSinkConnectorConfig(properties));
    PowerMockito.doReturn(1024).when(connectorConfig).getPartSize();
    topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    parsedConfig = new HashMap<>(connectorConfig.plainValues());
    compatibility = StorageSchemaCompatibility.getCompatibility(
                    connectorConfig.getString(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG));
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  public Storage newGcsClient(GcsSinkConnectorConfig config) {
    return null;
  }
}

