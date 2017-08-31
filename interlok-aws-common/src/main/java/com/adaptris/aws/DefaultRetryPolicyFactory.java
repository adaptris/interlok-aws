package com.adaptris.aws;

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.core.util.Args;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.thoughtworks.xstream.annotations.XStreamAlias;

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
  private PredefinedPolicy retryPolicy;
  
  public DefaultRetryPolicyFactory() {
    setRetryPolicy(PredefinedPolicy.DEFAULT);
  }
  
  @Override
  public RetryPolicy build() {
    return getRetryPolicy().build();
  }

  public PredefinedPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public void setRetryPolicy(PredefinedPolicy p) {
    this.retryPolicy = Args.notNull(p, "retryPolicy");
  }

}
