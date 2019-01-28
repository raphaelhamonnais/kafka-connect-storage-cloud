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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.confluent.connect.gcs.GcsSinkConnectorConfig;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Output stream enabling multi-part uploads of Kafka records.
 *
 * <p>The implementation has borrowed the general structure of Hadoop's implementation.
 */
public class GcsOutputStream extends OutputStream {
  private static final Logger log = LoggerFactory.getLogger(GcsOutputStream.class);
  private final Storage gcs;
  private final String bucket;
  private final String key;
  private final int partSize;
  private boolean closed;
  private ByteBuffer buffer;
  private MultipartUpload multiPartUpload;
  private final int retries;
  private final CompressionType compressionType;
  private volatile OutputStream compressionFilter;

  /*
  FIXME
  Temporary using a ByteArrayOutputStream "infinite" buffer instead of a
  limited sized ByteBuffer because the logic isn't quite right and it's
  losing data along the way.
  The ByteArrayOutputStream buffer will eventually throw OutOfMemory errors if
  the buffered data is too big.
   */
  private ByteArrayOutputStream infiniteBuffer; // Temporary hack until
  private boolean useInfiniteBuffer = true;

  public GcsOutputStream(String key, GcsSinkConnectorConfig conf, Storage gcs) {
    this.gcs = gcs;
    this.bucket = conf.getBucketName();
    this.key = key;
    this.partSize = conf.getPartSize();
    this.closed = false;
    this.retries = conf.getGcsPartRetries();
    this.buffer = ByteBuffer.allocate(this.partSize);
    this.infiniteBuffer = new ByteArrayOutputStream();
    this.multiPartUpload = null;
    this.compressionType = conf.getCompressionType();
    log.debug("Create GcsOutputStream for bucket '{}' key '{}'", bucket, key);
  }

  @Override
  public void write(int b) throws IOException {
    if (useInfiniteBuffer) {
      infiniteBuffer.write(b);
      return;
    }

    buffer.put((byte) b);
    if (!buffer.hasRemaining()) {
      uploadPart();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (useInfiniteBuffer) {
      infiniteBuffer.write(Arrays.copyOfRange(b, off, off + len));
      return;
    }

    if (buffer.remaining() <= len) {
      int firstPart = buffer.remaining();
      buffer.put(b, off, firstPart);
      uploadPart();
      write(b, off + firstPart, len - firstPart);
    } else {
      buffer.put(b, off, len);
    }
  }

  private static boolean outOfRange(int off, int len) {
    return off < 0 || off > len;
  }

  private void uploadPart() throws IOException {
    if (multiPartUpload == null) {
      log.debug("New multi-part upload for bucket '{}' key '{}'", bucket, key);
      multiPartUpload = newMultipartUpload();
    }
    try {
      multiPartUpload.uploadPart(buffer.array());
    } catch (Exception e) {
      if (multiPartUpload != null) {
        multiPartUpload.abort();
        log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
      }
      throw new IOException("Part upload failed: ", e.getCause());
    } finally {
      buffer.clear();
    }
  }

  public void commit() throws IOException {
    if (closed) {
      log.warn(
          "Tried to commit data for bucket '{}' key '{}' on a closed stream. Ignoring.",
          bucket,
          key
      );
      return;
    }

    try {
      compressionType.finalize(compressionFilter);
      if (!useInfiniteBuffer && buffer.hasRemaining()) {
        log.info("In commit() method, buffer has remaining data, uploading it.");
        uploadPart();
      }
      // Adding this check to make the ByteArrayOutputStream buffer work
      // because the multiPartUpload class was previously initialized when calling
      // the uploadPart() method, which is never called with the ByteArrayOutputStream buffer.
      if (multiPartUpload == null) {
        log.debug("New multi-part upload for bucket '{}' key '{}'", bucket, key);
        multiPartUpload = newMultipartUpload();
      }
      multiPartUpload.complete();
      log.debug("Upload complete for bucket '{}' key '{}'", bucket, key);
    } catch (Exception e) {
      log.error("Multipart upload failed to complete for bucket '{}' key '{}'", bucket, key);
      throw new DataException("Multipart upload failed to complete.", e);
    } finally {
      multiPartUpload = null;
      close();
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (multiPartUpload != null) {
      multiPartUpload.abort();
      log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
    }
    super.close();
  }

  private MultipartUpload newMultipartUpload() throws IOException {
    return new MultipartUpload();
  }

  private class MultipartUpload {
    private final BlobId uploadId;
    private final List<String> parts;
    private final List<String> partChecksums; // Not used yet.

    public MultipartUpload() {
      this.uploadId = BlobId.of(bucket, key);
      this.parts = new ArrayList<>();
      this.partChecksums = new ArrayList<>();
      log.debug(
          "Initiated multi-part upload for bucket '{}' key '{}' with id '{}'",
          bucket,
          key,
          uploadId
      );
    }

    public void uploadPart(byte[] content) {
      // TODO: current limit is 32 non-composed parts per compose objects.
      // Need to create a compose object per 32 uploads for a limit of 1024 total.
      int currentPartNumber = partChecksums.size() + 1;
      String partKey = key + ".part" + currentPartNumber;
      parts.add(partKey);
      BlobInfo uploadReq = BlobInfo.newBuilder(bucket, partKey).build();
      gcs.create(uploadReq, content);
      log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
      partChecksums.add(uploadReq.getCrc32c());
    }

    public void complete() {
      log.debug("Completing multi-part upload for key '{}', id '{}'", key, uploadId);

      if (useInfiniteBuffer) {
        gcs.create(BlobInfo.newBuilder(bucket, key).build(), infiniteBuffer.toByteArray());
        try {
          infiniteBuffer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        return;
      }

      // Use BlobInfo for target objects from the beginning to be able to set target options such
      // as crc32 and other in the future.
      BlobInfo composeReq = BlobInfo.newBuilder(bucket, key).build();
      gcs.compose(Storage.ComposeRequest.of(parts, composeReq));
    }

    public void abort() {
      log.warn("Aborting multi-part upload with id '{}'", uploadId);
      // TBD
      // FIXME code abort method
    }
  }

  public OutputStream wrapForCompression() {
    if (compressionFilter == null) {
      // Initialize compressionFilter the first time this method is called.
      compressionFilter = compressionType.wrapForOutput(this);
    }
    return compressionFilter;
  }
}
