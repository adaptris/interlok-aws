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

import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.util.NumberUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Basic {@link RetryPolicy} builder implementation for AWS that allows you to plug in your own conditions and strategies.
 *
 * @config aws2-pluggable-retry-policy-factory
 * @since 4.3.0
 */
@XStreamAlias("aws2-pluggable-retry-policy-factory")
public class PluggableRetryPolicyFactory implements RetryPolicyFactory {

  /**
   * The class name for your retry-condition.
   */
  @NotBlank
  @Getter
  @Setter
  private String retryConditionClass;
  /**
   * The class name for your backoff strategy
   */
  @NotBlank
  @Getter
  @Setter
  private String backoffStrategyClass;
  /**
   * Passed through as {@link RetryPolicy#numRetries()}.
   */
  @Min(0)
  @InputFieldDefault(value = "0")
  @Getter
  @Setter
  private Integer maxErrorRetry;
  /**
   * Passed through as {@link RetryPolicy##isMaxErrorRetryInClientConfigHonored()}.
   */
  @InputFieldDefault(value = "true")
  @Getter
  @Setter
  private Boolean useClientConfigurationMaxErrorRetry;

  public PluggableRetryPolicyFactory() {

  }

  @Override
  public RetryPolicy build() {
    RetryPolicy.Builder builder = RetryPolicy.builder();
    builder.backoffStrategy((BackoffStrategy)newInstance(getBackoffStrategyClass()));
    builder.numRetries(maxErrorRetry());
    builder.retryCapacityCondition((RetryCondition)newInstance(getRetryConditionClass()));
    return builder.build();
  }

  public PluggableRetryPolicyFactory withRetryConditionClass(String s) {
    setRetryConditionClass(s);
    return this;
  }

  public PluggableRetryPolicyFactory withBackoffStrategyClass(String s) {
    setBackoffStrategyClass(s);
    return this;
  }

  private Object newInstance(String s) {
    Object o = null;
    try {
      return !isEmpty(s) ? Class.forName(s).getDeclaredConstructor().newInstance() : null;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public PluggableRetryPolicyFactory withMaxErrorRetry(Integer i) {
    setMaxErrorRetry(i);
    return this;
  }

  int maxErrorRetry() {
    return NumberUtils.toIntDefaultIfNull(getMaxErrorRetry(), 0);
  }


  public PluggableRetryPolicyFactory withUseClientConfigurationMaxErrorRetry(Boolean b) {
    setUseClientConfigurationMaxErrorRetry(b);
    return this;
  }

  boolean useClientConfigurationMaxErrorRetry() {
    return BooleanUtils.toBooleanDefaultIfNull(getUseClientConfigurationMaxErrorRetry(), true);
  }

}
