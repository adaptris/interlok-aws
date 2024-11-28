package com.adaptris.aws;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryPolicy.RetryCondition;

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
    RetryCondition condition = policy.getRetryCondition();
    AmazonServiceException se1 = new AmazonServiceException("msg");
    se1.setStatusCode(HttpStatus.SC_NOT_FOUND);
    // It's a 404
    assertTrue(condition.shouldRetry(null, se1, 0));
    

    AmazonClientException clientException = new AmazonClientException("msg", new IOException());
    // From the code, a nested IOException is always retry-able.
    assertTrue(condition.shouldRetry(null, clientException, 0));

    AmazonServiceException se2 = new AmazonServiceException("msg", new IOException());
    // Oooh a 401
    se1.setStatusCode(HttpStatus.SC_UNAUTHORIZED);
    // BUT.. nested IOException so should be true.
    assertTrue(condition.shouldRetry(null, se2, 0));
  }

}
