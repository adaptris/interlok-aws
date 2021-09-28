/*
 * Copyright 2018 Adaptris
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.adaptris.aws2.s3;

import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.common.InputStreamWithEncoding;
import com.adaptris.core.common.PayloadStreamOutputParameter;
import com.adaptris.interlok.config.DataOutputParameter;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Get an object from S3 and store the contents of the object either in the message payload or
 * metadata.
 *
 * @author gdries
 * @config amazon-s3-get
 */
@XStreamAlias("aws2-amazon-s3-get")
@DisplayOrder(
    order = {"bucket", "objectName", "responseBody", "userMetadataFilter"})
public class S3GetOperation extends TransferOperation {

  /**
   * Where to write the contents of the get operation.
   * <p>
   * If not explicitly specified then defaults to the payload via
   * {@link PayloadStreamOutputParameter}.
   * </p>
   */
  @NotNull
  @Getter
  @Setter
  @Valid
  @InputFieldDefault(value = "the payload")
  private DataOutputParameter<InputStreamWithEncoding> responseBody;

  public S3GetOperation() {
    setResponseBody(new PayloadStreamOutputParameter());
  }

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    S3Client s3 = wrapper.amazonClient();

    GetObjectRequest.Builder builder = GetObjectRequest.builder();
    builder.bucket(s3Bucket(msg));
    builder.key(s3ObjectKey(msg));
    GetObjectRequest request = builder.build();
    log.debug("Getting {} from bucket {}", request.key(), request.bucket());

    ResponseInputStream<GetObjectResponse> responseStream = s3.getObject(request);
    GetObjectResponse response = responseStream.response();

    log.trace("Object is {} bytes", response.contentLength());
    getResponseBody().insert(new InputStreamWithEncoding(responseStream, null), msg);
    msg.setMetadata(filterUserMetadata(response.metadata()));
  }
}
