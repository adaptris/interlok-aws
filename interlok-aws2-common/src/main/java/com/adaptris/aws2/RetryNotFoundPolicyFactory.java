/*
    Copyright 2018 Adaptris

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.aws2;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryOnStatusCodeCondition;

import java.util.HashSet;
import java.util.Set;

/**
 * Allows you to configure additional HTTP codes that determine whether a retry should be attempted or not.
 * <p>
 * Sometimes you end up in a situation where you get a 404 exception even though the file exists (and invocations merely a few
 * seconds later are successful). This may well be down to a timing issue and/or issues with a transparent proxy configuration. Use
 * this builder to allow you automatically retry operations based on a 404.
 * </p>
 * 
 * @config aws2-retry-not-found-policy-factory
 */
@XStreamAlias("aws2-retry-not-found-policy-factory")
public class RetryNotFoundPolicyFactory implements RetryPolicyFactory {

  private static final Set<Integer> STATUS_CODES = new HashSet<>(5);

  static {
    STATUS_CODES.add(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    STATUS_CODES.add(HttpStatus.SC_BAD_GATEWAY);
    STATUS_CODES.add(HttpStatus.SC_SERVICE_UNAVAILABLE);
    STATUS_CODES.add(HttpStatus.SC_GATEWAY_TIMEOUT);
    STATUS_CODES.add(HttpStatus.SC_NOT_FOUND);
  }

  private static final int DEFAULT_MAX_ERROR_RETRY = 3; /* was PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY */

  @Override
  public RetryPolicy build() {

    RetryPolicy.Builder builder = RetryPolicy.builder();
    builder.backoffStrategy(BackoffStrategy.defaultStrategy());
    builder.retryCapacityCondition(RetryOnStatusCodeCondition.create(STATUS_CODES));
    builder.numRetries(DEFAULT_MAX_ERROR_RETRY);

    return builder.build();
  }
}
