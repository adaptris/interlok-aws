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

import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;

public class ClientConfigurationBuilder {

  private static transient Logger log = LoggerFactory.getLogger(ClientConfigurationBuilder.class);

  public static ClientOverrideConfiguration build(KeyValuePairSet settings) throws Exception {
    return build(settings, new DefaultRetryPolicyFactory());
  }

  public static ClientOverrideConfiguration build(KeyValuePairSet settings, RetryPolicyFactory b) throws Exception {
    return configure(settings, b.build());
  }

  public static ClientOverrideConfiguration configure(KeyValuePairSet settings, RetryPolicy retry) throws Exception
  {
    ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();
    builder.retryPolicy(retry);
    for (KeyValuePair kvp : settings.getKeyValuePairs())
    {
      String key = kvp.getKey();
      String value = kvp.getValue();

      builder.putHeader(key, value);
    }
    return builder.build();
  }
}

