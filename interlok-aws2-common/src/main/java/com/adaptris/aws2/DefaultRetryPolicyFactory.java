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

import com.adaptris.annotation.AutoPopulated;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;

import javax.validation.constraints.NotNull;

/**
 * The default retry policy builder using one of the predefined policies from {@link RetryPolicy}.
 *
 * @config aws2-default-retry-policy-factory
 * @since 4.3.0
 */
@XStreamAlias("aws2-default-retry-policy-factory")
public class DefaultRetryPolicyFactory implements RetryPolicyFactory {

  private static final int DYNAMODB_STANDARD_DEFAULT_MAX_ERROR_RETRY = 10; /* Was PredefinedRetryPolicies.DYNAMODB_STANDARD_DEFAULT_MAX_ERROR_RETRY */

  public enum PredefinedPolicy {
    /**
     * Uses PredefinedRetryPolicies#getDefaultRetryPolicy()
     *
     */
    DEFAULT {

      @Override
      RetryPolicy build() {
        return RetryPolicy.defaultRetryPolicy();
      }

    },
    /**
     * Uses PredefinedRetryPolicies#getDynamoDBDefaultRetryPolicy()
     *
     */
    DYNAMO_DB {
      @Override
      RetryPolicy build() {

        RetryPolicy.Builder builder = RetryPolicy.builder();
        builder.retryCondition(RetryCondition.defaultRetryCondition());
        builder.backoffStrategy(BackoffStrategy.defaultThrottlingStrategy());
        builder.numRetries(DYNAMODB_STANDARD_DEFAULT_MAX_ERROR_RETRY);

        return builder.build();
      }
    },
    NONE {
      @Override
      RetryPolicy build() {
        return RetryPolicy.none();
      }
    };
    abstract RetryPolicy build();
  }

  @NotNull
  @AutoPopulated
  @NonNull
  @Getter
  @Setter
  private PredefinedPolicy retryPolicy;

  public DefaultRetryPolicyFactory() {
    setRetryPolicy(PredefinedPolicy.DEFAULT);
  }

  public DefaultRetryPolicyFactory(PredefinedPolicy p) {
    this();
    setRetryPolicy(p);
  }

  @Override
  public RetryPolicy build() {
    return getRetryPolicy().build();
  }
}
