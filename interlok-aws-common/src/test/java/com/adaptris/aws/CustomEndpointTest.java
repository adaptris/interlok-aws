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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import com.amazonaws.client.builder.AwsClientBuilder;

public class CustomEndpointTest  {

  @Test
  public void testIsConfigured() throws Exception {
    CustomEndpoint c = new CustomEndpoint();
    assertFalse(c.isConfigured());
    c.setServiceEndpoint("http://blah");
    assertFalse(c.isConfigured());
    c.setSigningRegion("us-west-1");
    assertTrue(c.isConfigured());
  }

  @Test
  public void testServiceEndpoint() throws Exception {
    CustomEndpoint c = new CustomEndpoint();
    assertNull(c.getServiceEndpoint());
    c.setServiceEndpoint("http://blah");
    assertEquals("http://blah", c.getServiceEndpoint());
  }

  @Test
  public void testSigningRegion() throws Exception {    
    CustomEndpoint c = new CustomEndpoint();
    assertNull(c.getSigningRegion());
    c.setSigningRegion("us-west-1");
    assertEquals("us-west-1", c.getSigningRegion());
  }
  
  @Test
  public void testRebuild() throws Exception {    
    CustomEndpoint c = new CustomEndpoint().withServiceEndpoint("http://localhost:4567").withSigningRegion("us-west-1");
    AwsClientBuilder b = c.rebuild((AwsClientBuilder) new MockAwsClientBuilder());
    assertNotNull(b.getEndpoint());
    assertEquals("http://localhost:4567", b.getEndpoint().getServiceEndpoint());
    assertEquals("us-west-1", b.getEndpoint().getSigningRegion());
  }
  
}
