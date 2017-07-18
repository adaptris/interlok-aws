package com.adaptris.aws.sqs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.amazonaws.regions.Regions;

public class AwsHelperTest {

  @Test
  public void testGetRegion() throws Exception {
    assertEquals(Regions.EU_WEST_1.getName(), AwsHelper.formatRegion("eu-west-1"));
    assertEquals(Regions.EU_WEST_1.getName(), AwsHelper.formatRegion("EU-WEST-1"));
    assertEquals(Regions.EU_WEST_1.getName(), AwsHelper.formatRegion("amazon.eu-west-1"));
  }
}
