package com.adaptris.aws2;

import org.apache.http.HttpStatus;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RetryNotFoundPolicyFactoryTest {

  @Test
  public void testBuild() throws Exception {
    RetryNotFoundPolicyFactory retry = new RetryNotFoundPolicyFactory();
    assertNotNull(retry.build());
  }

  @Test
  public void testPolicy() throws Exception {
    RetryNotFoundPolicyFactory retry = new RetryNotFoundPolicyFactory();
    RetryPolicy policy = retry.build();
    RetryCondition condition = policy.retryCondition();
    AwsServiceException se1 = AwsServiceException.builder().statusCode(HttpStatus.SC_NOT_FOUND).message("msg").build();
    // It's a 404
    RetryPolicyContext.Builder builder = RetryPolicyContext.builder();
    builder.httpStatusCode(HttpStatus.SC_NOT_FOUND);
    builder.exception(se1);
    assertTrue(condition.shouldRetry(builder.build()));


    AwsServiceException clientException = AwsServiceException.builder().message("msg").cause(new IOException()).build();
    // From the code, a nested IOException is NO LONGER retry-able.
    builder = RetryPolicyContext.builder();
    assertFalse(condition.shouldRetry(builder.build()));

    AwsServiceException se2 = AwsServiceException.builder().statusCode(HttpStatus.SC_UNAUTHORIZED).message("msg").cause(new IOException()).build();
    // Oooh a 401
    builder = RetryPolicyContext.builder();
    builder.httpStatusCode(HttpStatus.SC_UNAUTHORIZED);
    builder.exception(se2);
    assertFalse(condition.shouldRetry(builder.build()));
  }

}
