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

package io.confluent.connect.gcs.storage;

import com.google.api.gax.retrying.RetrySettings;
import io.confluent.connect.gcs.GcsSinkConnectorTestBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.gcs.GcsSinkConnectorConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class GCSStorageTest extends GcsSinkConnectorTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();


  /**
   * Maximum retry limit.
   **/
  public static final int MAX_RETRIES = 30;

  protected Map<String, String> localProps = new HashMap<>();
  protected RetrySettings retrySettings;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    retrySettings = GcsStorage.retrySettings(connectorConfig);
  }

  @Test
  public void testRetryPolicy() throws Exception {
    assertTrue(retrySettings.isJittered());
    assertEquals((int) connectorConfig.getInt(GCS_PART_RETRIES_CONFIG), retrySettings.getMaxAttempts());
    assertEquals((long) connectorConfig.getLong(GCS_RETRY_BACKOFF_CONFIG), retrySettings.getInitialRetryDelay().toMillis());
    assertEquals(GCS_RETRY_MAX_BACKOFF_TIME_MS, retrySettings.getMaxRetryDelay().toMillis());
  }

// TODO implements retries unit/integration tests to make sure that the client performs retries
//  @Test
//  public void testRetryPolicyNonRetriable() throws Exception {
//    AmazonClientException e = new AmazonClientException("Non-retriable exception");
//    assertFalse(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
//  }
//
//  @Test
//  public void testRetryPolicyRetriableServiceException() throws Exception {
//    AmazonServiceException e = new AmazonServiceException("Retriable exception");
//    e.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
//    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
//  }
//
//  @Test
//  public void testRetryPolicyNonRetriableServiceException() throws Exception {
//    AmazonServiceException e = new AmazonServiceException("Non-retriable exception");
//    e.setStatusCode(HttpStatus.SC_METHOD_NOT_ALLOWED);
//    assertFalse(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
//  }
//
//  @Test
//  public void testRetryPolicyRetriableThrottlingException() throws Exception {
//    AmazonServiceException e = new AmazonServiceException("Retriable exception");
//    e.setErrorCode("TooManyRequestsException");
//    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
//  }
//
//  @Test
//  public void testRetryPolicyRetriableSkewException() throws Exception {
//    AmazonServiceException e = new AmazonServiceException("Retriable exception");
//    e.setErrorCode("RequestExpired");
//    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
//  }
//
//  @Test
//  public void testRetryPolicyDelayRanges() throws Exception {
//    assertComputeRetryInRange(10, 10L);
//    assertComputeRetryInRange(10, 100L);
//    assertComputeRetryInRange(10, 1000L);
//    assertComputeRetryInRange(MAX_RETRIES + 1, 1000L);
//    assertComputeRetryInRange(100, S3_RETRY_MAX_BACKOFF_TIME_MS + 1);
//    assertComputeRetryInRange(MAX_RETRIES + 1, S3_RETRY_MAX_BACKOFF_TIME_MS + 1);
//  }
//
//  /**
//   * Calculates exponential delay, capped by
//   * {@link com.amazonaws.retry.PredefinedBackoffStrategies#MAX_RETRIES} number of retries
//   * and {@link io.confluent.connect.s3.S3SinkConnectorConfig#S3_RETRY_MAX_BACKOFF_TIME_MS} total delay time
//   * in ms
//   *
//   * @param retriesAttempted
//   * @param baseDelay
//   * @return
//   * @see PredefinedBackoffStrategies#calculateExponentialDelay(int, int, int)
//   */
//  private int calculateExponentialDelay(
//      int retriesAttempted, long baseDelay
//  ) {
//    int retries = Math.min(retriesAttempted, MAX_RETRIES);
//    return (int) Math.min((1L << retries) * baseDelay, S3_RETRY_MAX_BACKOFF_TIME_MS);
//  }
//
//  private void assertComputeRetryInRange(
//      int retryAttempts,
//      long retryBackoffMs
//  ) throws Exception {
//
//    localProps.put(S3_RETRY_BACKOFF_CONFIG, String.valueOf(retryBackoffMs));
//    setUp();
//    RetryPolicy.BackoffStrategy backoffStrategy = retryPolicy.getBackoffStrategy();
//
//    for (int i = 0; i != 20; ++i) {
//      for (int retries = 0; retries <= retryAttempts; ++retries) {
//        long maxResult = calculateExponentialDelay(retries, retryBackoffMs);
//        long result = backoffStrategy.delayBeforeNextRetry(null, null, retries);
//        if (retryBackoffMs < 0) {
//          assertEquals(0, result);
//        } else {
//          assertTrue(result >= 0L);
//          assertTrue(result <= maxResult);
//        }
//      }
//    }
//  }
}
