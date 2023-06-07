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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.util.Os;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

public class ConnectionFromPropertiesTest extends ConnectionFromProperties {
  
  @Test
  public void testConfigLocation() {
    ConnectionFromProperties conn = new ConnectionFromProperties();
    assertNull(conn.getConfigLocation());
    assertNotNull(conn.withConfigLocation("kinesis_default.properties").getConfigLocation());
    try {
      conn.setConfigLocation(null);
      fail();
    } catch (Exception expected) {

    }
  }

  @Test
  public void testLifecycle() throws Exception {
    ConnectionFromProperties conn = new ConnectionFromProperties().withConfigLocation("kinesis_default.properties");
    try {
      LifecycleHelper.initAndStart(conn);
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
  }

  @Test
  public void testKinesisProducer() throws Exception {
    // On Windows the "kinesis_producer" executable is often missing
    assumeFalse(Os.isFamily(Os.WINDOWS_FAMILY));
    ConnectionFromProperties conn = new ConnectionFromProperties().withConfigLocation("kinesis_default.properties");
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

  @Test
  public void testShutdownQuietly() throws Exception {
    KinesisProducer mock = Mockito.mock(KinesisProducer.class);
    shutdownQuietly(null);
    shutdownQuietly(mock);
  }

  @Test
  public void testReadConfig() throws Exception {
    assertNotNull(readConfig("kinesis_default.properties"));
    try {
      readConfig("blahblahb-blahblah.properties");
      fail();
    } catch (IOException e) {

    }
  }
}
