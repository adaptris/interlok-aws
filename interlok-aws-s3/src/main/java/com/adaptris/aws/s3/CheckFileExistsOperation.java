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

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.services.exception.ExceptionHandlingServiceWrapper;
import com.amazonaws.services.s3.AmazonS3Client;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * Check an exists in S3 and throw an exception if it doesn't.
 * <p>
 * Note that this component will throw an exception if the file does not exist possibly cascading
 * into a failed message; you probably want to use something like
 * {@link ExceptionHandlingServiceWrapper} or similar if a missing S3 object is part of your
 * expected integration pipeline.
 * </p>
 * 
 * @config amazon-s3-check-file-exists
 */
@AdapterComponent
@ComponentProfile(summary = "Check a file exists in S3, throws exception if it doesn't",
    since = "3.8.4")
@XStreamAlias("amazon-s3-check-file-exists")
@DisplayOrder(order ={ "bucket", "objectName", "bucketName", "key"})
@NoArgsConstructor
public class CheckFileExistsOperation extends ObjectOperationImpl {


  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String bucket = s3Bucket(msg);
    String key = s3ObjectKey(msg);
    if (!s3.doesObjectExist(bucket, key)) {      
      throw new Exception(String.format("[%s:%s] does not exist", bucket, key));
    } else {
      log.trace("[{}:{}] exists", bucket, key);
    }
  }
  
}
