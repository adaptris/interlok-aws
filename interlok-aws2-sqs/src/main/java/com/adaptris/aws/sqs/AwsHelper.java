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

import lombok.NoArgsConstructor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

import java.net.URI;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang3.StringUtils.isEmpty;

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
    
    for (Region r : Region.regions()) {
      if (r.id().equalsIgnoreCase(actual)){
        return r.id();
      }
    }
    return region;
  }

  // If it's RFC2396 then make the assumption that the configurator has been quite explicit
  // about it all and we don't have to issue a GetQueueUrlRequest since that has "permissions"
  // associated with it.
  public static String buildQueueUrl(String queueName, String ownerAccount, SqsAsyncClient sqs) throws ExecutionException, InterruptedException
  {
    if (isValidURL(queueName)) {
      return queueName;
    }
    GetQueueUrlRequest.Builder queueUrlRequest = GetQueueUrlRequest.builder();
    queueUrlRequest.queueName(queueName);
    if (!isEmpty(ownerAccount)) {
      queueUrlRequest.queueOwnerAWSAccountId(ownerAccount);
    }
    return sqs.getQueueUrl(queueUrlRequest.build()).get().queueUrl();
  }

  public static String buildQueueUrl(String queueName, String ownerAccount, SqsClient sqs) throws ExecutionException, InterruptedException
  {
    if (isValidURL(queueName)) {
      return queueName;
    }
    GetQueueUrlRequest.Builder queueUrlRequest = GetQueueUrlRequest.builder();
    queueUrlRequest.queueName(queueName);
    if (!isEmpty(ownerAccount)) {
      queueUrlRequest.queueOwnerAWSAccountId(ownerAccount);
    }
    return sqs.getQueueUrl(queueUrlRequest.build()).queueUrl();
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
