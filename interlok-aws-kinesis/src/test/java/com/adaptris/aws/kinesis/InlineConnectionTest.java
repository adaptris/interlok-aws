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

package com.adaptris.aws.kinesis;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

public class InlineConnectionTest extends ConnectionFromProperties {

  @Test
  public void testLifecycle() throws Exception {
    InlineProducerConfiguration conn = new InlineProducerConfiguration();
    try {
      LifecycleHelper.initAndStart(conn);
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
  }

  @Test
  public void testKinesisProducer() throws Exception {
    InlineProducerConfiguration conn = new InlineProducerConfiguration()
        .withCredentials(new StaticCredentialsBuilder()).withMetricsCredentials(null)
        .withConfig(new KeyValuePairSet());
    try {
      LifecycleHelper.initAndStart(conn);
      KinesisProducer prod = conn.kinesisProducer();
      assertNotNull(prod);
      KinesisProducer p2 = conn.kinesisProducer();
      assertTrue(prod == p2);
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
  }
}
