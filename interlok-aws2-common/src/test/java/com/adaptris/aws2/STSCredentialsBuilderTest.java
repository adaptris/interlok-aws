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

import static org.junit.Assert.assertNotNull;
import java.util.Arrays;
import org.junit.Test;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;

public class STSCredentialsBuilderTest {

  @Test
  public void testBuild() throws Exception {

    STSAssumeroleCredentialsBuilder builder =
        new STSAssumeroleCredentialsBuilder().withRoleArn("arn:aws2:sts:us-west-1:123456789012:MyArn")
            .withRoleExternalId("externalId").withRoleSessionName("sessionName")
            .withCredentials(
                new StaticCredentialsBuilder()
                    .withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey")));
    builder.setScopeDownPolicy("{}");
    builder.setRoleDurationSeconds(10);
    builder.setTransitiveTagKeys(Arrays.asList("transitive1", "transitive2", "tt3"));
    KeyValuePairSet sessionTags = new KeyValuePairSet();
    sessionTags.add(new KeyValuePair("tag1", "value1"));
    sessionTags.add(new KeyValuePair("tag2", "value2"));
    builder.setSessionTags(sessionTags);

    // Since we're using build() w/o additional cfg, we need to spoof
    // DefaultAwsRegionProviderChain.
    boolean needsClearing = false;
    if (System.getProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY) == null) {
      System.setProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY, "us-west-1");
      needsClearing=true;
    }
    AWSCredentialsProvider credentials = builder.build();
    if (needsClearing) {
      System.clearProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY);
    }
    assertNotNull(credentials);
  }


  @Test
  public void testBuild_WithConfig() throws Exception {
    STSAssumeroleCredentialsBuilder builder =
        new STSAssumeroleCredentialsBuilder().withRoleArn("arn:aws2:sts:us-west-1:123456789012:MyArn")
        .withRoleExternalId("externalId").withRoleSessionName("sessionName");
    AWSCredentialsProvider credentials = builder.build(new DummyConfig());
    assertNotNull(credentials);
  }


  private class DummyConfig implements AWSCredentialsProviderBuilder.BuilderConfig {

    @Override
    public KeyValuePairSet clientConfiguration() {
      return new KeyValuePairSet();
    }

    @Override
    public EndpointBuilder endpointBuilder() {
      return new CustomEndpoint().withServiceEndpoint("https://sts.us-west-1.amazonaws.com")
          .withSigningRegion("us-west-1");
    }

    @Override
    public RetryPolicyFactory retryPolicy() {
      return new DefaultRetryPolicyFactory();
    }

  }
}
