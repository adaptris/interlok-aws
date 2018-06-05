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
