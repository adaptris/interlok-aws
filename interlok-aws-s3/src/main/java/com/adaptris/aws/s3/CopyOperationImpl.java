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

package com.adaptris.aws.s3;

import javax.validation.constraints.NotBlank;
import com.adaptris.core.AdaptrisMessage;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Copy an object from S3 to another object
 *
 */
@NoArgsConstructor
public abstract class CopyOperationImpl extends ObjectOperationImpl {

  /**
   * The destination bucket.
   * <p>
   * If not explictly configured, then we use the bucket name instead making the assumption it's a
   * copy within the same bucket.
   * </p>
   */
  @Getter
  @Setter
  private String destinationBucket;

  /**
   * The destination object.
   */
  @Getter
  @Setter
  @NotBlank
  private String destinationObjectName;



  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    CopyObjectRequest copyReq = createCopyRequest(wrapper, msg);
    log.trace("Copying [{}:{}] to [{}:{}]", copyReq.getSourceBucketName(), copyReq.getSourceKey(),
        copyReq.getDestinationBucketName(), copyReq.getDestinationKey());
    s3.copyObject(copyReq);
  }

  protected abstract CopyObjectRequest createCopyRequest(ClientWrapper wrapper, AdaptrisMessage msg)
      throws Exception;

  @SuppressWarnings("unchecked")
  public <T extends CopyOperationImpl> T withDestinationBucket(String s) {
    setDestinationBucket(s);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public <T extends CopyOperationImpl> T withDestinationObjectName(String s) {
    setDestinationObjectName(s);
    return (T) this;
  }
}
