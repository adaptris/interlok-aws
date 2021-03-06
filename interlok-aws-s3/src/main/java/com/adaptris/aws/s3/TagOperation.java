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

import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * Add tags to an object from S3 to another object
 *
 * <p>
 * Uses {@link AmazonS3Client#setObjectTagging(SetObjectTaggingRequest)}.
 * </p>
 *
 * @config amazon-s3-copy
 */
@AdapterComponent
@ComponentProfile(summary = "Tag an object in S3")
@XStreamAlias("amazon-s3-tag")
@DisplayOrder(order = {"bucket", "objectName", "tagMetadataFilter"})
@NoArgsConstructor
public class TagOperation extends ObjectOperationImpl {

  @Valid
  private MetadataFilter tagMetadataFilter;

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String srcBucket = s3Bucket(msg);
    String srcKey = s3ObjectKey(msg);
    List<Tag> tags = filterTagMetadata(msg);
    if (!tags.isEmpty()) {
      log.trace("Tagging [{}:{}]", srcBucket, srcKey);
      s3.setObjectTagging(new SetObjectTaggingRequest(srcBucket, srcKey, new ObjectTagging(tags)));
    }
  }

  public MetadataFilter getTagMetadataFilter() {
    return tagMetadataFilter;
  }

  /**
   * Filter metadata and set them as tags for the s3 object.
   *
   * @param mf the metadata filter; if not specified defaults to {@link RemoveAllMetadataFilter}.
   */
  public void setTagMetadataFilter(MetadataFilter mf) {
    tagMetadataFilter = mf;
  }

  public <T extends TagOperation> T withTagMetadataFilter(MetadataFilter mf) {
    setTagMetadataFilter(mf);
    return (T) this;
  }


  protected MetadataFilter tagMetadataFilter() {
    return ObjectUtils.defaultIfNull(getTagMetadataFilter(), new RemoveAllMetadataFilter());
  }

  protected List<Tag> filterTagMetadata(AdaptrisMessage msg) {
    MetadataCollection metadata = tagMetadataFilter().filter(msg);
    List<Tag> result = new ArrayList<>(metadata.size());
    for (MetadataElement e : metadata) {
      result.add(new Tag(e.getKey(), e.getValue()));
    }
    return result;
  }

}
