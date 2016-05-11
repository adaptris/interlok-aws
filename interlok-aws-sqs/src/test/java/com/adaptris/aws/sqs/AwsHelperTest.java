package com.adaptris.aws.sqs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.adaptris.aws.sqs.AwsHelper;
import com.adaptris.core.CoreException;
import com.amazonaws.regions.Regions;

public class AwsHelperTest {

  @Test
  public void testGetRegion() throws Exception {
    assertEquals(Regions.EU_WEST_1.getName(), AwsHelper.formatRegion("eu-west-1").getName());
    assertEquals(Regions.EU_WEST_1.getName(), AwsHelper.formatRegion("EU-WEST-1").getName());
    assertEquals(Regions.EU_WEST_1.getName(), AwsHelper.formatRegion("amazon.eu-west-1").getName());
  }

  @Test
  public void testNotFound() throws Exception {
    try {
      AwsHelper.formatRegion(this.getClass().getCanonicalName());
    } catch (CoreException expected) {
      
    }
  }
}
