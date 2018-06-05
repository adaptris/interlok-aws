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

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.common.InputStreamWithEncoding;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataOutputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Get an object from S3 and store the contents of the object either in the message payload or metadata.
 * 
 * @author gdries
 * @config amazon-s3-get
 */
@XStreamAlias("amazon-s3-get")
@DisplayOrder(order = {"bucketName", "key", "responseBody", "userMetadataFilter"})
public class S3GetOperation extends TransferOperation {

  @NotNull
  private DataOutputParameter<InputStreamWithEncoding> responseBody;


  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws InterlokException {
    AmazonS3Client s3 = wrapper.amazonClient();
    GetObjectRequest request = new GetObjectRequest(getBucketName().extract(msg), getKey().extract(msg));
    log.debug("Getting {} from bucket {}", request.getKey(), request.getBucketName());
    S3Object response = s3.getObject(request);
    log.trace("Object is {} bytes", response.getObjectMetadata().getContentLength());
    getResponseBody().insert(new InputStreamWithEncoding(response.getObjectContent(), null), msg);
    msg.setMetadata(filterUserMetadata(response.getObjectMetadata().getUserMetadata()));
  }

  public DataOutputParameter<InputStreamWithEncoding> getResponseBody() {
    return responseBody;
  }

  public void setResponseBody(DataOutputParameter<InputStreamWithEncoding> responseBody) {
    this.responseBody = responseBody;
  }
}
