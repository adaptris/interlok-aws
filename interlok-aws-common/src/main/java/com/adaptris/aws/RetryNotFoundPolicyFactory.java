package com.adaptris.aws;

import java.util.HashSet;
import java.util.Set;

import org.apache.http.HttpStatus;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Allows you to configure additional HTTP codes that determine whether a retry should be attempted or not.
 * <p>
 * Sometimes you end up in a situation where you get a 404 exception even though the file exists (and invocations merely a few
 * seconds later are successful). This may well be down to a timing issue and/or issues with a transparent proxy configuration. Use
 * this builder to allow you automatically retry operations based on a 404.
 * </p>
 * 
 * @config aws-retry-not-found-policy-factory
 */
@XStreamAlias("aws-retry-not-found-policy-factory")
public class RetryNotFoundPolicyFactory implements RetryPolicyFactory {

  private static final Set<Integer> STATUS_CODES = new HashSet<Integer>(5);

  static {
    STATUS_CODES.add(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    STATUS_CODES.add(HttpStatus.SC_BAD_GATEWAY);
    STATUS_CODES.add(HttpStatus.SC_SERVICE_UNAVAILABLE);
    STATUS_CODES.add(HttpStatus.SC_GATEWAY_TIMEOUT);
    STATUS_CODES.add(HttpStatus.SC_NOT_FOUND);
  }

  @Override
  public RetryPolicy build() {
    // defaults and obey client-config-max-errors
    return new RetryPolicy(new Allow404(), PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
        PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY, true);
  }

  private class Allow404 extends PredefinedRetryPolicies.SDKDefaultRetryCondition {
    @Override
    public boolean shouldRetry(AmazonWebServiceRequest orig, AmazonClientException exc, int attempts) {
      if (exc instanceof AmazonServiceException) {
        AmazonServiceException ase = (AmazonServiceException) exc;
        if (STATUS_CODES.contains(ase.getStatusCode())) return true;
      }
      return super.shouldRetry(orig, exc, attempts);
    }
  }
}
