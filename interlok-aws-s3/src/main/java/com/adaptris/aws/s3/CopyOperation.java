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

import javax.validation.Valid;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Copy an object from S3 to another object
 * 
 * <p>
 * Uses {@link AmazonS3Client#copyObject(String, String, String, String)} using only the default
 * behaviour.
 * </p>
 * 
 * @config amazon-s3-copy
 */
@AdapterComponent
@ComponentProfile(summary = "Copy an object in S3 to another Object")
@XStreamAlias("amazon-s3-copy")
@DisplayOrder(order = {"bucket", "objectName", "destinationBucket", "destinationObjectName",
    "bucketName", "key", "destinationBucketName", "destinationKey"})
@NoArgsConstructor
public class CopyOperation extends ObjectOperationImpl {
  private transient boolean bucketNameWarningLogged = false;
  private transient boolean keyNameWarningLogged = false;

  /**
   * The destination bucket.
   * <p>
   * If not explictly configured, then we use the bucket name instead making the assumption it's a
   * copy within the same bucket.
   * </p>
   */
  @Valid
  @Getter
  @Setter
  @Deprecated
  @Removal(version = "3.12.0", message = "Use an expression based bucket instead")
  private DataInputParameter<String> destinationBucketName;
  /**
   * The destination object.
   */
  @Valid
  @Getter
  @Setter  
  @Deprecated
  @Removal(version = "3.12.0", message = "Use an expression based key instead")
  private DataInputParameter<String> destinationKey;
  
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
  // @NotBlank because of deprecated thing.
  private String destinationObjectName;


  @Override
  public void prepare() throws CoreException {
    super.prepare();
    if (getDestinationBucketName() != null) {
      LoggingHelper.logWarning(bucketNameWarningLogged, () -> bucketNameWarningLogged = true,
          "[{}] uses [destination-bucket-name], use [destination-bucket] instead",
          this.getClass().getSimpleName());
    }
    if (getDestinationKey() != null) {
      LoggingHelper.logWarning(keyNameWarningLogged, () -> keyNameWarningLogged = true,
          "[{}] uses [destination-key], use the alternative string-based expression instead",
          this.getClass().getSimpleName());
    }
    mustHaveEither(getDestinationKey(), getDestinationObjectName());
  }

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String srcBucket = s3Bucket(msg);
    String srcKey = s3ObjectKey(msg);
    String destBucket = destinationBucket(msg);
    String destKey = destinationKey(msg);
    log.trace("Copying [{}:{}] to [{}:{}]", srcBucket, srcKey, destBucket, destKey);
    s3.copyObject(srcBucket, srcKey, destBucket, destKey);
  }

  @Deprecated
  public CopyOperation withDestinationBucketName(DataInputParameter<String> bucket) {
    setDestinationBucketName(bucket);
    return this;
  }


  @Deprecated
  public CopyOperation withDestinationKey(DataInputParameter<String> key) {
    setDestinationKey(key);
    return this;
  }

  public CopyOperation withDestinationBucket(String s) {
    setDestinationBucket(s);
    return this;
  }

  public CopyOperation withDestinationObjectName(String s) {
    setDestinationObjectName(s);
    return this;
  }


  private String destinationKey(AdaptrisMessage msg) throws InterlokException {
    return resolve(getDestinationKey(), getDestinationObjectName(), msg);
  }

  private String destinationBucket(AdaptrisMessage msg) throws InterlokException {
    return ObjectUtils.defaultIfNull(
        resolve(getDestinationBucketName(), getDestinationBucket(), msg), s3Bucket(msg));
  }
}
