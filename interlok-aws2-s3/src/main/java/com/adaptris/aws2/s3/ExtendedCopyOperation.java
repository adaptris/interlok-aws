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

package com.adaptris.aws2.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws2.s3.meta.S3ObjectMetadata;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.interlok.util.Args;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    S3Client s3 = wrapper.amazonClient();
    String srcBucket = s3Bucket(msg);
    String srcKey = s3ObjectKey(msg);
    String destBucket = ObjectUtils.defaultIfNull(msg.resolve(getDestinationBucket()), srcBucket);
    String destKey = msg.resolve(getDestinationObjectName());

    CopyObjectRequest.Builder builder = CopyObjectRequest.builder();
    builder.sourceBucket(srcBucket);
    builder.sourceKey(srcKey);
    builder.destinationBucket(destBucket);
    builder.destinationKey(destKey);
    builder.metadata(buildNewMetadata(s3, msg, srcBucket, srcKey));
    builder.tagging(buildNewTags(s3, srcBucket, srcKey));

    return builder.build();
  }

  private Tagging buildNewTags(S3Client s3, String srcBucket, String srcKey) {
    GetObjectTaggingRequest.Builder taggingRequestBuilder = GetObjectTaggingRequest.builder();
    taggingRequestBuilder.bucket(srcBucket);
    taggingRequestBuilder.key(srcKey);
    GetObjectTaggingResponse taggingResponse = s3.getObjectTagging(taggingRequestBuilder.build());

    List<Tag> combined = new ArrayList<>(taggingResponse.tagSet());
    Tagging.Builder tagging = Tagging.builder();
    for (KeyValuePair k : overrideTags()) {
      combined.add(Tag.builder().key(k.getKey()).value(k.getValue()).build());
    }
    tagging.tagSet(combined);
    return tagging.build();
  }

  private Map<String, String> buildNewMetadata(S3Client s3, AdaptrisMessage msg, String srcBucket, String srcKey) throws Exception {

    HeadObjectRequest.Builder builder = HeadObjectRequest.builder();
    builder.bucket(srcBucket);
    builder.key(srcKey);

    HeadObjectResponse response = s3.headObject(builder.build());
    Map<String, String> combined = new HashMap<>(response.metadata());

    for (S3ObjectMetadata m : overrideMetadata()) {
      m.apply(msg, combined);
    }
    return combined;
  }

  public ExtendedCopyOperation withObjectMetadata(List<S3ObjectMetadata> meta) {
    setObjectMetadata(meta);
    return this;
  }

  public ExtendedCopyOperation withObjectMetadata(S3ObjectMetadata... meta) {
    return withObjectMetadata(new ArrayList(Arrays.asList(meta)));
  }

  public ExtendedCopyOperation withObjectTags(KeyValuePairSet tags) {
    setObjectTags(tags);
    return this;
  }

  public ExtendedCopyOperation withObjectTags(KeyValuePair... tags) {
    return withObjectTags(new KeyValuePairSet(Arrays.asList(tags)));
  }


  private List<S3ObjectMetadata> overrideMetadata() {
    return ObjectUtils.defaultIfNull(getObjectMetadata(), Collections.EMPTY_LIST);
  }

  private KeyValuePairSet overrideTags() {
    return ObjectUtils.defaultIfNull(getObjectTags(), new KeyValuePairSet());
  }
}
