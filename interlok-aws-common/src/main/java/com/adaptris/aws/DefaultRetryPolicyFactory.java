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

package com.adaptris.aws;

import javax.validation.constraints.NotNull;
import com.adaptris.annotation.AutoPopulated;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * The default retry policy builder using one of the predefined policies from {@link PredefinedRetryPolicies}.
 * 
 * @config aws-default-retry-policy-factory
 */
@XStreamAlias("aws-default-retry-policy-factory")
public class DefaultRetryPolicyFactory implements RetryPolicyFactory {

  public enum PredefinedPolicy {
    /**
     * Uses PredefinedRetryPolicies#getDefaultRetryPolicy()
     * 
     */
    DEFAULT {

      @Override
      RetryPolicy build() {
        return PredefinedRetryPolicies.getDefaultRetryPolicy();
      }
      
    },
    /**
     * Uses PredefinedRetryPolicies#getDynamoDBDefaultRetryPolicy()
     * 
     */
    DYNAMO_DB {
      @Override
      RetryPolicy build() {
        return PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicy();

      }
    },
    NONE {
      @Override
      RetryPolicy build() {
        return PredefinedRetryPolicies.NO_RETRY_POLICY;
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
