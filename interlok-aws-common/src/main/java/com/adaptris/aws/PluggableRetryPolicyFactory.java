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

import static org.apache.commons.lang.StringUtils.isEmpty;
import javax.validation.constraints.Min;
import org.apache.commons.lang3.BooleanUtils;
import org.hibernate.validator.constraints.NotBlank;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.util.NumberUtils;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryPolicy.BackoffStrategy;
import com.amazonaws.retry.RetryPolicy.RetryCondition;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Basic {@link RetryPolicy} builder implementation for AWS that allows you to plug in your own conditions and strategies.
 * 
 * @config aws-pluggable-retry-policy-factory
 */
@XStreamAlias("aws-pluggable-retry-policy-factory")
public class PluggableRetryPolicyFactory implements RetryPolicyFactory {

  @NotBlank
  private String retryConditionClass;
  @NotBlank
  private String backoffStrategyClass;
  @Min(0)
  @InputFieldDefault(value = "0")
  private Integer maxErrorRetry;
  @InputFieldDefault(value = "true")
  private Boolean useClientConfigurationMaxErrorRetry;

  public PluggableRetryPolicyFactory() {

  }

  @Override
  public RetryPolicy build() {
    return new RetryPolicy((RetryCondition) newInstance(getRetryConditionClass()),
        (BackoffStrategy) newInstance(getBackoffStrategyClass()), maxErrorRetry(), useClientConfigurationMaxErrorRetry());
  }

  public String getRetryConditionClass() {
    return retryConditionClass;
  }

  /**
   * Set the class name for your retry-condition.
   * 
   * @param clazz the class name, null if not specified.
   */
  public void setRetryConditionClass(String clazz) {
    this.retryConditionClass = clazz;
  }

  public PluggableRetryPolicyFactory withRetryConditionClass(String s) {
    setRetryConditionClass(s);
    return this;
  }

  public String getBackoffStrategyClass() {
    return backoffStrategyClass;
  }

  /**
   * Set the class name for your backoff strategy
   * 
   * @param clazz the class name, null if not specified.
   */
  public void setBackoffStrategyClass(String clazz) {
    this.backoffStrategyClass = clazz;
  }

  public PluggableRetryPolicyFactory withBackoffStrategyClass(String s) {
    setBackoffStrategyClass(s);
    return this;
  }

  private Object newInstance(String s) {
    Object o = null;
    try {
      return !isEmpty(s) ? Class.forName(s).newInstance() : null;
    }
    catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }


  public Integer getMaxErrorRetry() {
    return maxErrorRetry;
  }

  /**
   * Equivalent to {@link RetryPolicy#getMaxErrorRetry()}.
   * 
   * @param max the value; defaults to 0
   */
  public void setMaxErrorRetry(Integer max) {
    this.maxErrorRetry = max;
  }

  public PluggableRetryPolicyFactory withMaxErrorRetry(Integer i) {
    setMaxErrorRetry(i);
    return this;
  }

  int maxErrorRetry() {
    return NumberUtils.toIntDefaultIfNull(getMaxErrorRetry(), 0);
  }

  public Boolean getUseClientConfigurationMaxErrorRetry() {
    return useClientConfigurationMaxErrorRetry;
  }

  /**
   * Equivalent to {@link RetryPolicy#isMaxErrorRetryInClientConfigHonored()}.
   * 
   * @param b true or false, defaults to true.
   */
  public void setUseClientConfigurationMaxErrorRetry(Boolean b) {
    this.useClientConfigurationMaxErrorRetry = b;
  }

  public PluggableRetryPolicyFactory withUseClientConfigurationMaxErrorRetry(Boolean b) {
    setUseClientConfigurationMaxErrorRetry(b);
    return this;
  }

  boolean useClientConfigurationMaxErrorRetry() {
    return BooleanUtils.toBooleanDefaultIfNull(getUseClientConfigurationMaxErrorRetry(), true);
  }

}
