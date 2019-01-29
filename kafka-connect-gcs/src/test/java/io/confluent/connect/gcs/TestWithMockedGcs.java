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

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import io.confluent.connect.gcs.format.avro.AvroUtils;
import io.confluent.connect.gcs.format.bytearray.ByteArrayUtils;
import io.confluent.connect.gcs.format.json.JsonUtils;
import io.confluent.connect.gcs.storage.CompressionType;
import io.confluent.connect.gcs.storage.GcsStorage;
import io.confluent.connect.gcs.util.FileUtils;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class TestWithMockedGcs extends GcsSinkConnectorTestBase {

  protected static final String PATH_TO_GOOGLE_CREDENTIALS = "/Users/raphael.hamonnais/Downloads/datadog-sandbox-68abd4e4d7cf.json";
  public static final String PROJECT_ID = "datadog-sandbox";
  protected static final Logger log = LoggerFactory.getLogger(TestWithMockedGcs.class);

  protected static Storage staticGcsClient = getStaticGcsClient();

  @Rule
  public TemporaryFolder gcsMockRoot = new TemporaryFolder();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "_");
    props.put(StorageCommonConfig.FILE_DELIM_CONFIG, "#");
    return props;
  }

  /**
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    if (isRealClient()) {
      log.info("Using a real GCS client: creating bucket if need be");
      if (staticGcsClient.get(GCS_TEST_BUCKET_NAME) != null) {
        log.info("Bucket {} already exists, removing all files and versions before tests", GCS_TEST_BUCKET_NAME);
        staticGcsClient.list(GCS_TEST_BUCKET_NAME, BlobListOption.prefix(""), BlobListOption.versions(true))
                       .iterateAll().forEach(b -> b.delete());
      } else {
        log.info("Creating bucket {} for unit tests", GCS_TEST_BUCKET_NAME);
        staticGcsClient.create(BucketInfo.newBuilder(GCS_TEST_BUCKET_NAME).setVersioningEnabled(true).build());
      }
      assertNotNull(staticGcsClient.get(GCS_TEST_BUCKET_NAME));
    }
    else {
      log.info("Not using a real GCS client, no need to create a bucket");
    }
  }

  //@Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (isRealClient()) {
      log.info("Cleaning up GCS resources in {}", GCS_TEST_BUCKET_NAME);
      // Removing all files in the bucket because the unit tests expect to have only a given
      // set of files in the bucket per test and will fail if there are other files present.
      staticGcsClient.list(GCS_TEST_BUCKET_NAME, Storage.BlobListOption.prefix(""), Storage.BlobListOption.versions(true))
                     .iterateAll().forEach(b -> b.delete());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (isRealClient()) {
      log.info("Cleaning up GCS resources in {}", GCS_TEST_BUCKET_NAME);
      staticGcsClient.list(GCS_TEST_BUCKET_NAME, BlobListOption.prefix(""), BlobListOption.versions(true))
                     .iterateAll().forEach(b -> b.delete());
      log.info("Deleting the bucket {}", GCS_TEST_BUCKET_NAME);
      staticGcsClient.get(GCS_TEST_BUCKET_NAME).delete();
    }
  }


  public static List<Blob> listObjects(String bucket, String prefix, Storage gcs) {
    List<Blob> objects = new ArrayList<>();
    Page<Blob> listing;
    try {
      if (prefix == null) {
        listing = gcs.list(bucket);
      } else {
        listing = gcs.list(bucket, Storage.BlobListOption.prefix(prefix));
      }
      listing.iterateAll().forEach(objects::add);
    } catch (StorageException e) {
     log.warn("listObjects for bucket '{}' prefix '{}' returned error code: {}", bucket, prefix, e.getCode());
     e.printStackTrace();
    }
    return objects;
  }

  public static Collection<Object> readRecords(String topicsDir, String directory, TopicPartition tp, long startOffset,
                                               String extension, String zeroPadFormat, String bucketName, Storage gcs) throws IOException {
      String fileKey = FileUtils.fileKeyToCommit(topicsDir, directory, tp, startOffset,
          extension, zeroPadFormat);
      CompressionType compressionType = CompressionType.NONE;
      if (extension.endsWith(".gz")) {
        compressionType = CompressionType.GZIP;
      }
      if (".avro".equals(extension)) {
        return readRecordsAvro(bucketName, fileKey, gcs);
      } else if (extension.startsWith(".json")) {
        return readRecordsJson(bucketName, fileKey, gcs, compressionType);
      } else if (extension.startsWith(".bin")) {
        return readRecordsByteArray(bucketName, fileKey, gcs, compressionType,
            GcsSinkConnectorConfig.FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT.getBytes());
      } else if (extension.startsWith(".customExtensionForTest")) {
        return readRecordsByteArray(bucketName, fileKey, gcs, compressionType,
            "SEPARATOR".getBytes());
      } else {
        throw new IllegalArgumentException("Unknown extension: " + extension);
      }
  }

  public static Collection<Object> readRecordsAvro(String bucketName, String fileKey, Storage gcs) throws IOException {
      log.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
    InputStream in = inputStream(gcs, bucketName, fileKey);
    return AvroUtils.getRecords(in);
  }

  public static Collection<Object> readRecordsJson(String bucketName, String fileKey, Storage gcs,
                                                   CompressionType compressionType) throws IOException {
      log.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
    InputStream in = inputStream(gcs, bucketName, fileKey);
    return JsonUtils.getRecords(compressionType.wrapForInput(in));
  }

  public static Collection<Object> readRecordsByteArray(String bucketName, String fileKey, Storage gcs,
                                                        CompressionType compressionType, byte[] lineSeparatorBytes) throws IOException {
      log.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
      InputStream in = inputStream(gcs, bucketName, fileKey);
      return ByteArrayUtils.getRecords(compressionType.wrapForInput(in), lineSeparatorBytes);
  }

  protected static InputStream inputStream(Storage gcsClient, String bucket, String key) {
    Blob obj = getObject(gcsClient, bucket, key);
    InputStream in = new ByteArrayInputStream(obj.getContent());
    return new BufferedInputStream(in);
  }

  protected static Blob getObject(Storage gcsClient, String bucket, String key) {
    return gcsClient.get(BlobId.of(bucket, key));
  }

  public Storage newGcsClient(GcsSinkConnectorConfig config) {
    Storage realGCSClient = null;

    // Try instantiating a real GCS Client if proper credential and project id are specified
    try {
      realGCSClient = StorageOptions.newBuilder()
                                    .setProjectId(PROJECT_ID)
                                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(PATH_TO_GOOGLE_CREDENTIALS)))
                                    .setRetrySettings(GcsStorage.retrySettings(config))
                                    .build()
                                    .getService();
    } catch (IOException ignored) {}

    // Return a mocked Storage Helper if the real GCS Client couldn't be instantiated
    if (realGCSClient == null)
      return LocalStorageHelper.getOptions().getService();

    return realGCSClient;
  }

  private static Storage getStaticGcsClient() {
    // Try instantiating a real GCS Client if proper credential and project id are specified
    // and return null if not.
    Storage realGCSClient = null;
    try {
      realGCSClient = RemoteStorageHelper
          .create(PROJECT_ID, new FileInputStream(PATH_TO_GOOGLE_CREDENTIALS))
          .getOptions()
          .getService();
    } catch (IOException ignored) {}
    return realGCSClient;
  }

  protected static boolean isRealClient() {
    try {
      new FileInputStream(PATH_TO_GOOGLE_CREDENTIALS);
    } catch (FileNotFoundException e) {
      return false;
    }
    return true;
  }
}
