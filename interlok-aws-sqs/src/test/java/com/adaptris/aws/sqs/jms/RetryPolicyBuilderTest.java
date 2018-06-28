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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.adaptris.aws.sqs.jms.RetryPolicyBuilder;
import com.amazonaws.retry.RetryPolicy;

public class RetryPolicyBuilderTest {

  @Test
  public void testBuild() throws Exception {
    RetryPolicyBuilder b = new RetryPolicyBuilder();
    b.setBackoffStrategyClass(MyBackoffStrategy.class.getCanonicalName());
    b.setRetryConditionClass(MyRetryCondition.class.getCanonicalName());
    b.setMaxErrorRetry(10);
    b.setUseClientConfigurationMaxErrorRetry(true);
    RetryPolicy rp = b.build();
    assertEquals(MyBackoffStrategy.class, rp.getBackoffStrategy().getClass());
    assertEquals(MyRetryCondition.class, rp.getRetryCondition().getClass());
    assertEquals(10, rp.getMaxErrorRetry());
    assertTrue(rp.isMaxErrorRetryInClientConfigHonored());
  }

  @Test
  public void testBuildNullImpls() throws Exception {
    RetryPolicyBuilder b = new RetryPolicyBuilder();
    assertNotNull(b.build());
  }

  @Test
  public void testBuildBadImpls() throws Exception {
    RetryPolicyBuilder b = new RetryPolicyBuilder();
    try {
      b.setBackoffStrategyClass("blahblah");
      b.build();
      fail();
    } catch (Exception expected) {

    }
  }
}
