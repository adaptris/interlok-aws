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

package com.adaptris.aws.s3;

import java.util.List;
import java.util.Set;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.Tag;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Check a file exists in S3 and throw an exception if it doesn't.
 * 
 * 
 * @config amazon-s3-check-file-exists
 */
@AdapterComponent
@ComponentProfile(summary = "Check a file exists in S3")
@XStreamAlias("amazon-s3-check-file-exists")
@DisplayOrder(order ={ "bucketName", "key"})
public class CheckFileExistsOperation extends S3OperationImpl {

  public CheckFileExistsOperation() {
  }

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String bucket = getBucketName().extract(msg);
    String key = getKey().extract(msg);
    if (!s3.doesObjectExist(bucket, key)) {      
      throw new Exception(String.format("[%s:%s] does not exist", bucket, key));
    } else {
      log.trace("[{}:{}] exists", bucket, key);
    }
  }
  
}
