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

package com.adaptris.aws.sqs.jms;

import static org.apache.commons.lang.StringUtils.isEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.aws.PluggableRetryPolicyFactory;
import com.adaptris.aws.RetryPolicyFactory;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryPolicy.BackoffStrategy;
import com.amazonaws.retry.RetryPolicy.RetryCondition;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * {@link RetryPolicy} builder for Amazon SQS.
 * 
 * @config sqs-retry-policy-builder
 * @since 3.2.1
 * @deprecated since 3.6.4 use a {@link RetryPolicyFactory} and {@link PluggableRetryPolicyFactory} instead.
 */
@XStreamAlias("sqs-retry-policy-builder")
@Deprecated
public class RetryPolicyBuilder {

  private String retryConditionClass;
  private String backoffStrategyClass;
  @NotNull
  @Min(0)
  @AutoPopulated
  private Integer maxErrorRetry;
  private Boolean useClientConfigurationMaxErrorRetry;

  public RetryPolicyBuilder() {

  }

  public RetryPolicy build() throws Exception {
    return new RetryPolicy((RetryCondition) newInstance(getRetryConditionClass()),
        (BackoffStrategy) newInstance(getBackoffStrategyClass()), maxErrorRetry(), useClientConfigurationMaxErrorRetry());
  }

  public String getRetryConditionClass() {
    return retryConditionClass;
  }

  public void setRetryConditionClass(String retryConditionClass) {
    this.retryConditionClass = retryConditionClass;
  }

  public String getBackoffStrategyClass() {
    return backoffStrategyClass;
  }

  public void setBackoffStrategyClass(String s) {
    this.backoffStrategyClass = s;
  }

  private Object newInstance(String s) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    return (!isEmpty(s)) ? Class.forName(s).newInstance() : null;
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

  int maxErrorRetry() {
    return getMaxErrorRetry() != null ? getMaxErrorRetry().intValue() : 0;
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

  boolean useClientConfigurationMaxErrorRetry() {
    return getUseClientConfigurationMaxErrorRetry() != null ? getUseClientConfigurationMaxErrorRetry().booleanValue() : true;
  }

}
