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

package com.adaptris.aws2.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

/**
 * Delete an object from S3.
 *
 * @config amazon-s3-download
 * @since 4.3.0
 */
@AdapterComponent
@ComponentProfile(summary = "Delete an object from S3", since = "4.3.0")
@XStreamAlias("aws2-amazon-s3-delete")
@DisplayOrder(order = {"bucket", "objectName"})
@NoArgsConstructor
public class DeleteOperation extends ObjectOperationImpl {

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    S3Client s3 = wrapper.amazonClient();
    String bucket = s3Bucket(msg);
    String key = s3ObjectKey(msg);
    DeleteObjectRequest.Builder builder = DeleteObjectRequest.builder();
    builder.bucket(bucket);
    builder.key(key);
    log.trace("Deleting [{}:{}]", bucket, key);
    s3.deleteObject(builder.build());
  }

}
