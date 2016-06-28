package com.adaptris.aws.sqs;

import com.adaptris.core.CoreException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Adaptris helper class for Amazon SQS services.
 */
final class AwsHelper {

  private AwsHelper() {}
  
  /**
   * Format the Amazon region string by removing the unnecessary prefix if it exists.
   * 
   * @param region
   * @return region correctly formatted
   */
  public static Region formatRegion(String region) throws CoreException {
    String actual = "";
    String[] regionSplit = region.split("[.]");
    if (regionSplit.length == 1){
      actual = regionSplit[0];
    } else {
      actual = regionSplit[1];
    }
    
    for (Regions r : Regions.values()) {
      if (r.getName().equalsIgnoreCase(actual)){
        return Region.getRegion(r);
      }
    }
    
    throw new CoreException(String.format("Region %s not found", region));
  }
}
