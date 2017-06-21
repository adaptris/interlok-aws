package com.adaptris.aws.sqs;

import com.amazonaws.regions.Regions;

/**
 * Adaptris helper class for Amazon SQS services.
 */
final class AwsHelper {

  private AwsHelper() {}
  
  /**
   * Format the Amazon region string by removing the unnecessary prefix if it exists.
   * 
   */
  public static String formatRegion(String region) {
    String actual = "";
    String[] regionSplit = region.split("[.]");
    if (regionSplit.length == 1){
      actual = regionSplit[0];
    } else {
      actual = regionSplit[1];
    }
    
    for (Regions r : Regions.values()) {
      if (r.getName().equalsIgnoreCase(actual)){
        return r.getName();
      }
    }
    return region;
  }
}
