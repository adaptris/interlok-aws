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

import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.interlok.cloud.BlobListRenderer;
import com.adaptris.interlok.cloud.RemoteBlobFilter;
import com.adaptris.interlok.config.DataInputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
@DisplayOrder(order = {"bucket", "prefix", "bucketName", "key", "pageResults", "maxKeys", "filter",
    "filterSuffix"})
@NoArgsConstructor
public class ListOperation extends S3OperationImpl {
  private transient boolean suffixWarningLogged;
  private transient boolean pageWarningLogged;

  /**
   * Specific the prefix for use with the List operation.
   * 
   */
  @Getter
  @Setter
  @InputFieldHint(expression = true)
  private String prefix;

  @Getter
  @Setter
  @Deprecated
  @AdvancedConfig
  @Removal(version = "3.11.0", message = "Use a RemoteBlobFilter instead")
  private DataInputParameter<String> filterSuffix;

  /**
   * Specify any additional filtering you wish to perform on the list.
   * 
   */
  @AdvancedConfig
  @Getter
  @Setter
  private RemoteBlobFilter filter;

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

  /**
   * Specify whether to page over results.
   *
   * <p>
   *   If set to true will return all results, as oppose to the first n, where n is max-keys (AWS default: 1000).
   *   Default is false for backwards compatibility reasons.
   * </p>
   * @deprecated since 3.10.2 due to interface changes; paging results is not explicitly configurable and will be ignored.
   */
  @AdvancedConfig(rare = true)
  @Getter
  @Setter
  @Deprecated
  @Removal(version = "3.11.0",
      message = "due to interface changes; paging results is not explicitly configurable and will be ignored")
  private Boolean pageResults;

  /**
   * Specify max number of keys to be returned per page when paging through results.
   */
  @AdvancedConfig(rare=true)
  @Getter
  @Setter
  private Integer maxKeys;


  @Override
  public void prepare() throws CoreException {
    super.prepare();
    if (getFilterSuffix() != null) {
      LoggingHelper.logWarning(suffixWarningLogged, () -> {
        suffixWarningLogged = true;
      }, "[{}] uses filter-suffix; use a filter instead");
    }
    if (getPageResults() != null) {
      LoggingHelper.logWarning(pageWarningLogged, () -> pageWarningLogged = true,
          "[{}] uses [page-results], this is ignored",
          this.getClass().getSimpleName());
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String _bucket = s3Bucket(msg);
    String _prefix = resolve(getKey(), getPrefix(), msg);
    ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(_bucket).withPrefix(_prefix);
    if (getMaxKeys() != null) {
      request.setMaxKeys(getMaxKeys());
    }
    outputStyle().render(new RemoteBlobIterable(s3, request, blobFilter(msg)), msg);
  }

  @Deprecated
  @Removal(version = "3.11.0")
  public ListOperation withFilterSuffix(DataInputParameter<String> key) {
    setFilterSuffix(key);
    return this;
  }

  private BlobListRenderer outputStyle() {
    return ObjectUtils.defaultIfNull(getOutputStyle(), new BlobListRenderer() {});
  }

  public ListOperation withOutputStyle(BlobListRenderer render) {
    setOutputStyle(render);
    return this;
  }


  public ListOperation withFilter(RemoteBlobFilter filter) {
    setFilter(filter);
    return this;
  }

  @Deprecated
  @Removal(version = "3.11.0", message="due to interface changes; paging results is not explicitly configurable and will be ignored")
  public ListOperation withPageResults(Boolean paging){
    setPageResults(paging);
    return this;
  }

  public ListOperation withMaxKeys(Integer maxKeys){
    setMaxKeys(maxKeys);
    return this;
  }

  public ListOperation withPrefix(String prefix) {
    setPrefix(prefix);
    return this;
  }

  private RemoteBlobFilter blobFilter(AdaptrisMessage msg) throws Exception {
    if (getFilterSuffix() != null) {
      final String suffix = getFilterSuffix().extract(msg);
      return (blob) -> blob.getName().endsWith(suffix);
    }
    return ObjectUtils.defaultIfNull(getFilter(), (blob) -> true);
  }

}
