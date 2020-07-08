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

import java.util.List;
import java.util.Set;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

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
@XStreamAlias("amazon-s3-tag-get")
@DisplayOrder(order = {"bucket", "objectName", "bucketName", "key", "tagMetadataFilter"})
@NoArgsConstructor
public class GetTagOperation extends TagOperation {

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String srcBucket = s3Bucket(msg);
    String srcKey = s3ObjectKey(msg);
    log.trace("Getting tags for [{}:{}]", srcBucket, srcKey);
    GetObjectTaggingRequest req = new GetObjectTaggingRequest(srcBucket, srcKey);
    msg.setMetadata(filterTags(s3.getObjectTagging(req).getTagSet()));    
  }

  protected Set<MetadataElement> filterTags(List<Tag> tags) {
    MetadataCollection msgMetadata = new MetadataCollection();
    for (Tag tag : tags) {
      msgMetadata.add(new MetadataElement(tag.getKey(), tag.getValue()));
    }
    return tagMetadataFilter().filter(msgMetadata).toSet();
  }
  
}
