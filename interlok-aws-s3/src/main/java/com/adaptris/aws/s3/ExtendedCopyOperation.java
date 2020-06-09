/*
 * Copyright 2020 Adaptris
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.validation.Valid;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws.s3.meta.S3ObjectMetadata;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.interlok.util.Args;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Tag;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Copy an object from S3 to another object
 *
 * <p>
 * By default this operation gets the existing ObjectMetadata and Tags associated with the S3
 * object, and ensures that they are applied to the underlying {@code CopyObjectRequest}. You also
 * have the option to force various settings using the {@code object-metadata} and
 * {@code object-tags} members as required. If you're using the Amazon S3 API against a different
 * provider then your mileage may vary since {@code object-tags} and {@code object-metadata} might
 * not translate to alternate providers.
 * </p>
 *
 * @config amazon-s3-extended-copy
 */
@AdapterComponent
@ComponentProfile(summary = "Copy an object in S3 to another Object", since = "3.10.2")
@XStreamAlias("amazon-s3-extended-copy")
@DisplayOrder(order = {"bucket", "objectName", "destinationBucket", "destinationObjectName"})
@NoArgsConstructor
public class ExtendedCopyOperation extends CopyOperationImpl {

  /**
   * Any specific object metadata that you want to force on the destination object.
   */
  @Setter
  @Getter
  @AdvancedConfig
  @Valid
  private List<S3ObjectMetadata> objectMetadata;

  /**
   * Any specific object tags that you want to force on the destination object.
   */
  @Setter
  @Getter
  @AdvancedConfig
  @Valid
  private KeyValuePairSet objectTags;

  @Override
  public void prepare() throws CoreException {
    super.prepare();
    Args.notBlank(getDestinationObjectName(), "destination-object-name");
  }

  @Override
  protected CopyObjectRequest createCopyRequest(ClientWrapper wrapper, AdaptrisMessage msg)
      throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String srcBucket = s3Bucket(msg);
    String srcKey = s3ObjectKey(msg);
    String destBucket = ObjectUtils.defaultIfNull(msg.resolve(getDestinationBucket()), srcBucket);
    String destKey = msg.resolve(getDestinationObjectName());
    CopyObjectRequest copyReq = new CopyObjectRequest(srcBucket, srcKey, destBucket, destKey)
        .withNewObjectMetadata(buildNewMetadata(s3, msg, srcBucket, srcKey))
        .withNewObjectTagging(new ObjectTagging(buildNewTags(s3, srcBucket, srcKey)));
    return copyReq;
  }

  private List<Tag> buildNewTags(AmazonS3Client s3, String srcBucket, String srcKey) {
    GetObjectTaggingRequest req = new GetObjectTaggingRequest(srcBucket, srcKey);
    List<Tag> result = new ArrayList<>(s3.getObjectTagging(req).getTagSet());
    for (KeyValuePair k : overrideTags()) {
      result.add(new Tag(k.getKey(), k.getValue()));
    }
    return result;
  }

  private ObjectMetadata buildNewMetadata(AmazonS3Client s3, AdaptrisMessage msg,
      String srcBucket,
      String srcKey) throws Exception {
    ObjectMetadata result = s3.getObjectMetadata(srcBucket, srcKey).clone();
    for (S3ObjectMetadata m : overrideMetadata()) {
      m.apply(msg, result);
    }
    return result;
  }

  public ExtendedCopyOperation withObjectMetadata(List<S3ObjectMetadata> meta) {
    setObjectMetadata(meta);
    return this;
  }

  public ExtendedCopyOperation withObjectTags(KeyValuePairSet tags) {
    setObjectTags(tags);
    return this;
  }

  private List<S3ObjectMetadata> overrideMetadata() {
    return ObjectUtils.defaultIfNull(getObjectMetadata(), Collections.EMPTY_LIST);
  }

  private KeyValuePairSet overrideTags() {
    return ObjectUtils.defaultIfNull(getObjectTags(), new KeyValuePairSet());
  }
}
