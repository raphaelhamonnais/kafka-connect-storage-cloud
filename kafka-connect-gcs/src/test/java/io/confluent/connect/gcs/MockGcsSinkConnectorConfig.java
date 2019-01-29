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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

public class MockGcsSinkConnectorConfig extends GcsSinkConnectorConfig {
  public static final String TEST_PART_SIZE_CONFIG = "test.gcs.part.size";

  public static ConfigDef newConfigDef() {
    ConfigDef configDef = GcsSinkConnectorConfig.getConfig();
    final String group = "GCS Tests";
    int orderInGroup = 0;
    configDef.define(TEST_PART_SIZE_CONFIG,
                      Type.INT,
                      1024,
                      Importance.HIGH,
                      "Tests - The Part Size in GCS Multi-part Uploads for tests.",
                      group,
                      ++orderInGroup,
                      Width.MEDIUM,
                      "Tests - GCS Part Size for tests");
    return configDef;
  }

  public MockGcsSinkConnectorConfig(Map<String, String> props) {
    super(newConfigDef(), props);
  }

  @Override
  public int getPartSize() {
    // Depends on the following test-internal property to override partSize for tests.
    // If not set, throws an exception.
    return getInt(TEST_PART_SIZE_CONFIG);
  }
}
