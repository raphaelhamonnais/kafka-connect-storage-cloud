/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.gcs.storage;

import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.avro.file.SeekableInput;

import java.io.OutputStream;

import io.confluent.connect.gcs.GcsSinkConnectorConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import org.threeten.bp.Duration;

import static io.confluent.connect.gcs.GcsSinkConnectorConfig.GCS_PART_RETRIES_CONFIG;
import static io.confluent.connect.gcs.GcsSinkConnectorConfig.GCS_RETRY_BACKOFF_CONFIG;
import static io.confluent.connect.gcs.GcsSinkConnectorConfig.GCS_RETRY_MAX_BACKOFF_TIME_MS;

/**
 * GCS implementation of the storage interface for Connect sinks.
 */
public class GcsStorage
    implements io.confluent.connect.storage.Storage<GcsSinkConnectorConfig, Page<Blob>> {

  private final String url;
  private final String bucketName;
  private final Storage gcs;
  private final GcsSinkConnectorConfig conf;

  /**
   * Construct an Gcs storage class given a configuration and an GCS address.
   *
   * @param conf the GCS configuration.
   * @param url the Gcs address.
   */
  public GcsStorage(GcsSinkConnectorConfig conf, String url) {
    this.url = url;
    this.conf = conf;
    this.bucketName = conf.getBucketName();
    this.gcs = newGcsClient(conf);
  }

  /**
   * Construct a new GCS client given a configuration.
   *
   * @param config the GCS configuration.
   */
  public Storage newGcsClient(GcsSinkConnectorConfig config) {
    /*
     * Notes about features to implement if needed:
     *  - TODO Credentials (if needed)
     *  - TODO Proxy (if needed)
     *  - TODO Region (if needed)
     *    - According to https://cloud.google.com/storage/docs/locations, the region (or location)
     *      can only be specified when creating a bucket and not at the GCS client level.
     */
    return StorageOptions.newBuilder()
                         .setRetrySettings(retrySettings(config))
                         .build()
                         .getService();
  }

  public static RetrySettings retrySettings(GcsSinkConnectorConfig config) {
    return RetrySettings
        .newBuilder()
        .setMaxAttempts(config.getInt(GCS_PART_RETRIES_CONFIG))
        .setInitialRetryDelay(Duration.ofMillis(config.getLong(GCS_RETRY_BACKOFF_CONFIG)))
        .setMaxRetryDelay(Duration.ofMillis(GCS_RETRY_MAX_BACKOFF_TIME_MS))
        .setTotalTimeout(Duration.ofMillis(120000L))
        .setRetryDelayMultiplier(1.5)
        .setInitialRpcTimeout(Duration.ofMillis(120000L))
        .setRpcTimeoutMultiplier(1.5)
        .setMaxRpcTimeout(Duration.ofMillis(120000L))
        .build();
  }

  // Visible for testing.
  public GcsStorage(GcsSinkConnectorConfig conf, String url, String bucketName, Storage gcs) {
    this.url = url;
    this.conf = conf;
    this.bucketName = bucketName;
    this.gcs = gcs;
  }

  @Override
  public boolean exists(String name) {
    return StringUtils.isNotBlank(name)
        && gcs.get(bucketName, name, Storage.BlobGetOption.fields()) != null;

  }

  public boolean bucketExists() {
    return StringUtils.isNotBlank(bucketName)
        && gcs.get(bucketName, Storage.BucketGetOption.fields()) != null;
  }

  @Override
  public boolean create(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream create(String path, GcsSinkConnectorConfig conf, boolean overwrite) {
    return create(path, overwrite);
  }

  public GcsOutputStream create(String path, boolean overwrite) {
    if (!overwrite) {
      throw new UnsupportedOperationException(
          "Creating a file without overwriting is not currently supported in Gcs Connector"
      );
    }

    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException("Path can not be empty!");
    }

    // currently ignore what is passed as method argument.
    return new GcsOutputStream(path, this.conf, gcs);
  }

  @Override
  public OutputStream append(String filename) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String name) {
    if (bucketName.equals(name)) {
      return;
    } else {
      gcs.delete(bucketName, name);
    }
  }

  @Override
  public void close() {}

  @Override
  public Page<Blob> list(String path) {
    return gcs.list(
        bucketName,
        Storage.BlobListOption.currentDirectory(),
        Storage.BlobListOption.prefix(path)
    );
  }

  @Override
  public GcsSinkConnectorConfig conf() {
    return conf;
  }

  @Override
  public String url() {
    return url;
  }

  @Override
  public SeekableInput open(String path, GcsSinkConnectorConfig conf) {
    throw new UnsupportedOperationException(
        "File reading is not currently supported in GCS Connector"
    );
  }
}
