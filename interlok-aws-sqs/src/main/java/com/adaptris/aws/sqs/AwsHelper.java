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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import java.net.URI;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import lombok.NoArgsConstructor;

/**
 * Adaptris helper class for Amazon SQS services.
 */
@NoArgsConstructor
class AwsHelper {

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

  // If it's RFC2396 then make the assumption that the configurator has been quite explicit
  // about it all and we don't have to issue a GetQueueUrlRequest since that has "permissions"
  // associated with it.
  public static String buildQueueUrl(String queueName, String ownerAccount, AmazonSQS sqs) {
    if (isValidURL(queueName)) {
      return queueName;
    }
    GetQueueUrlRequest queueUrlRequest = new GetQueueUrlRequest(queueName);
    if (!isEmpty(ownerAccount)) {
      queueUrlRequest.withQueueOwnerAWSAccountId(ownerAccount);
    }
    return sqs.getQueueUrl(queueUrlRequest).getQueueUrl();
  }

  // while it isn't foolproof, it's probably enough...
  // https://https://example.com will parse this check.
  private static boolean isValidURL(String url) {
    try {
      URI uri = new URI(url).parseServerAuthority();
      if (uri.getScheme() == null) {
        throw new Exception();
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
