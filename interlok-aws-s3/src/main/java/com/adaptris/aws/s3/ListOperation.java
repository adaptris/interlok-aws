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

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.services.exception.ExceptionHandlingServiceWrapper;
import com.adaptris.interlok.config.DataInputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.http.util.Args;

import java.io.PrintStream;

/**
 * List of files based on S3 key.
 *
 * @config amazon-s3-check-file-exists
 */
@AdapterComponent
@ComponentProfile(summary = "List of files based on S3 key",
    since = "3.9.0")
@XStreamAlias("amazon-s3-list")
@DisplayOrder(order ={ "bucketName", "key", "filterSuffix"})
public class ListOperation extends S3OperationImpl {

  private DataInputParameter<String> filterSuffix;

  public ListOperation() {
  }

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String bucket = getBucketName().extract(msg);
    String key = getKey().extract(msg);
    String filterSuffix = "";
    if (getFilterSuffix() != null){
      filterSuffix = getFilterSuffix().extract(msg);
    }
    ObjectListing listing =  s3.listObjects(bucket, key);
    try (PrintStream ps = new PrintStream(msg.getOutputStream())) {
      for (S3ObjectSummary summary : listing.getObjectSummaries()) {
        if (summary.getKey().endsWith(filterSuffix)) {
          ps.println(summary.getKey());
        }
      }
    }
  }

  public DataInputParameter<String> getFilterSuffix() {
    return filterSuffix;
  }

  public void setFilterSuffix(DataInputParameter<String> filterSuffix) {
    this.filterSuffix = Args.notNull(filterSuffix, "filterSuffix");
  }

  public ListOperation withFilterSuffix(DataInputParameter<String> key) {
    setFilterSuffix(key);
    return this;
  }
}
