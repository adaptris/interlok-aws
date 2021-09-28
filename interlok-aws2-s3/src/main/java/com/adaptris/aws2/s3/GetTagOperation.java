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
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.Tag;

import java.util.List;
import java.util.Set;

/**
 * Get tags associated with a S3 Object
 *
 * <p>
 * Uses {@code AmazonS3Client#getObjectTagging(GetObjectTaggingRequest)}
 * </p>
 *
 * @config amazon-s3-tag-get
 */
@AdapterComponent
@ComponentProfile(summary = "Get tags associated with an object in S3", since = "3.8.4")
@XStreamAlias("aws2-amazon-s3-tag-get")
@DisplayOrder(order = {"bucket", "objectName", "tagMetadataFilter"})
@NoArgsConstructor
public class GetTagOperation extends TagOperation {

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    S3Client s3 = wrapper.amazonClient();
    String srcBucket = s3Bucket(msg);
    String srcKey = s3ObjectKey(msg);
    log.trace("Getting tags for [{}:{}]", srcBucket, srcKey);
    GetObjectTaggingRequest.Builder builder = GetObjectTaggingRequest.builder();
    builder.bucket(srcBucket);
    builder.key(srcKey);
    msg.setMetadata(filterTags(s3.getObjectTagging(builder.build()).tagSet()));
  }

  protected Set<MetadataElement> filterTags(List<Tag> tags) {
    MetadataCollection msgMetadata = new MetadataCollection();
    for (Tag tag : tags) {
      msgMetadata.add(new MetadataElement(tag.key(), tag.value()));
    }
    return tagMetadataFilter().filter(msgMetadata).toSet();
  }

}
