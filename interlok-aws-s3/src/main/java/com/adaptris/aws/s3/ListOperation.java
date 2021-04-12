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
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.interlok.cloud.BlobListRenderer;
import com.adaptris.interlok.cloud.RemoteBlobFilter;
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
@DisplayOrder(order = {"bucket", "prefix", "outputStyle", "maxKeys", "filter"})
@NoArgsConstructor
public class ListOperation extends S3OperationImpl {
  /**
   * Specific the prefix for use with the List operation.
   *
   */
  @Getter
  @Setter
  @InputFieldHint(expression = true)
  private String prefix;

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
   * Specify max number of keys to be returned per page when paging through results.
   */
  @AdvancedConfig(rare=true)
  @Getter
  @Setter
  private Integer maxKeys;


  @Override
  public void prepare() throws CoreException {
    super.prepare();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    AmazonS3Client s3 = wrapper.amazonClient();
    String _bucket = s3Bucket(msg);
    String _prefix = msg.resolve(getPrefix(), true);
    ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(_bucket).withPrefix(_prefix);
    if (getMaxKeys() != null) {
      request.setMaxKeys(getMaxKeys());
    }
    outputStyle().render(new RemoteBlobIterable(s3, request, blobFilter(msg)), msg);
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

  public ListOperation withMaxKeys(Integer maxKeys){
    setMaxKeys(maxKeys);
    return this;
  }

  public ListOperation withPrefix(String prefix) {
    setPrefix(prefix);
    return this;
  }

  private RemoteBlobFilter blobFilter(AdaptrisMessage msg) throws Exception {
    return ObjectUtils.defaultIfNull(getFilter(), (blob) -> true);
  }

}
