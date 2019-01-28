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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import io.confluent.connect.gcs.format.avro.AvroUtils;
import io.confluent.connect.gcs.storage.GcsStorage;
import io.confluent.connect.gcs.util.FileUtils;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest({GcsSinkTask.class, StorageFactory.class})
@PowerMockIgnore({"io.findify.gcsmock.*", "akka.*", "javax.*", "org.xml.*", "com.sun.org.apache.xerces.*"})
public class GcsSinkTaskTest extends DataWriterAvroTest {

  private static final String ZERO_PAD_FMT = "%010d";
  private final String extension = ".avro";

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    Capture<Class<GcsStorage>> capturedStorage = EasyMock.newCapture();
    Capture<Class<GcsSinkConnectorConfig>> capturedStorageConf = EasyMock.newCapture();
    Capture<GcsSinkConnectorConfig> capturedConf = EasyMock.newCapture();
    Capture<String> capturedUrl = EasyMock.newCapture();
    PowerMock.mockStatic(StorageFactory.class);
    EasyMock.expect(StorageFactory.createStorage(EasyMock.capture(capturedStorage),
                                                 EasyMock.capture(capturedStorageConf),
                                                 EasyMock.capture(capturedConf),
                                                 EasyMock.capture(capturedUrl))).andReturn(storage);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Test
  public void testTaskType() throws Exception {
    setUp();
    replayAll();
    task = new GcsSinkTask();
    SinkTask.class.isAssignableFrom(task.getClass());
  }

  @Test
  public void testWriteRecord() throws Exception {
    setUp();
    replayAll();
    task = new GcsSinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    List<SinkRecord> sinkRecords = createRecords(7);
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteRecordWithPrimitives() throws Exception {
    setUp();
    replayAll();
    task = new GcsSinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    List<SinkRecord> sinkRecords = createRecordsWithPrimitive(7, 0, Collections.singleton(new TopicPartition (TOPIC, PARTITION)));
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testRecoveryWithPartialFile() throws Exception {
    setUp();

    // Upload partial file.
    List<SinkRecord> sinkRecords = createRecords(2);
    byte[] partialData = AvroUtils.putRecords(sinkRecords, format.getAvroData());
    String fileKey = FileUtils.fileKeyToCommit(topicsDir, getDirectory(), TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT);
    gcs.create(BlobInfo.newBuilder(BlobId.of(GCS_TEST_BUCKET_NAME, fileKey)).build(), partialData);

    // Accumulate rest of the records.
    sinkRecords.addAll(createRecords(5, 2));

    replayAll();
    task = new GcsSinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteRecordsSpanningMultipleParts() throws Exception {
    localProps.put(GcsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "10000");
    setUp();

    List<SinkRecord> sinkRecords = createRecords(11000);
    replayAll();

    task = new GcsSinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 10000};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testPartitionerConfig() throws Exception {
    localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
    localProps.put("custom.partitioner.config", "arbitrary value");
    setUp();
    replayAll();
    task = new GcsSinkTask();
    task.initialize(context);
    task.start(properties);
  }

  public static class CustomPartitioner<T> extends DefaultPartitioner<T> {
    @Override
    public void configure(Map<String, Object> map) {
      assertTrue("Custom parameters were not passed down to the partitioner implementation",
          map.containsKey("custom.partitioner.config"));
    }
  }

  @Test
  @Ignore
  public void testAclCannedConfig() throws Exception {
    // TODO add ACL_CANNED_CONFIG support to GCS if needed
    // localProps.put(GcsSinkConnectorConfig.ACL_CANNED_CONFIG, CannedAccessControlList.BucketOwnerFullControl.toString());
    setUp();
    replayAll();
    task = new GcsSinkTask();
    task.initialize(context);
    task.start(properties);
  }

}

