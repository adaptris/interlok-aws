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
import java.util.Collection;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.interlok.cloud.BlobListRenderer;
import com.adaptris.interlok.cloud.RemoteBlob;
import com.adaptris.interlok.config.DataInputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

/**
 * List of files based on S3 key.
 *
 * @config amazon-s3-check-file-exists
 */
@AdapterComponent
@ComponentProfile(summary = "List of files based on S3 key",
    since = "3.9.1")
@XStreamAlias("amazon-s3-list")
@DisplayOrder(order ={ "bucketName", "key", "filterSuffix"})
public class ListOperation extends S3OperationImpl {

  @Getter
  @Setter
  private DataInputParameter<String> filterSuffix;

  /**
   * Specify the output style.
   * 
   * <p>
   * If left as null, then only the names of the files will be emitted. You may require additional
   * optional components to utilise other rendering styles.
   * </p>
   */
  @Getter
  @Setter
  private BlobListRenderer outputStyle;

  public ListOperation() {
  }

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String bucket = getBucketName().extract(msg);
    String key = getKey().extract(msg);
    ObjectListing listing =  s3.listObjects(bucket, key);
    filter(listing, msg);
    outputStyle().render(filter(listing, msg), msg);
  }

  private Collection<RemoteBlob> filter(ObjectListing listing, AdaptrisMessage msg) throws Exception {
    Collection<RemoteBlob> list = new ArrayList<>();
    String filterSuffix = "";
    if (getFilterSuffix() != null) {
      filterSuffix = getFilterSuffix().extract(msg);
    }
    for (S3ObjectSummary summary : listing.getObjectSummaries()) {
      if (summary.getKey().endsWith(filterSuffix)) {
        list.add(new RemoteBlob.Builder().setBucket(summary.getBucketName()).setLastModified(summary.getLastModified().getTime())
            .setName(summary.getKey()).setSize(summary.getSize()).build()
        );
      }
    }
    return list;
  }


  public ListOperation withFilterSuffix(DataInputParameter<String> key) {
    setFilterSuffix(key);
    return this;
  }

  private BlobListRenderer outputStyle() {
    return ObjectUtils.defaultIfNull(getOutputStyle(), new BlobListRenderer() {});
  }

}
