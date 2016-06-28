package com.adaptris.aws.sqs.jms;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy.RetryCondition;

public class MyRetryCondition implements RetryCondition {

  @Override
  public boolean shouldRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception, int retriesAttempted) {
    return false;
  }

}
