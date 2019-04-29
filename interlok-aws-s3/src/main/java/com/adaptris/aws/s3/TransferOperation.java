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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.validation.Valid;

import org.apache.commons.lang3.ObjectUtils;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;

/**
 * Abstract base class for S3 Upload/Download Operations.
 * 
 *
 */
public abstract class TransferOperation extends S3OperationImpl {

  @Valid
  @AdvancedConfig
  private MetadataFilter userMetadataFilter;

  public TransferOperation() {
  }

  public MetadataFilter getUserMetadataFilter() {
    return userMetadataFilter;
  }

  /**
   * Filter either S3 object user-metadata or AdaptrisMessage metadata (depending on the operation type).
   * 
   * <p>
   * Note that user-metadata for an object is limited by the HTTP request header limit. All HTTP headers included in a request
   * (including user metadata headers and other standard HTTP headers) must be less than 8KB.
   * </p>
   * 
   * @param mf the metadata filter; if not specified defaults to {@link RemoveAllMetadataFilter}.
   */
  public void setUserMetadataFilter(MetadataFilter mf) {
    this.userMetadataFilter = mf;
  }

  public <T extends TransferOperation> T withUserMetadataFilter(MetadataFilter mf) {
    setUserMetadataFilter(mf);
    return (T) this;
  }
  
  MetadataFilter userMetadataFilter() {
    return ObjectUtils.defaultIfNull(getUserMetadataFilter(), new RemoveAllMetadataFilter());
  }

  protected Set<MetadataElement> filterUserMetadata(Map<String, String> userMetadata) {
    MetadataCollection msgMetadata = new MetadataCollection();
    for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
      msgMetadata.add(new MetadataElement(entry.getKey(), entry.getValue()));
    }
    return userMetadataFilter().filter(msgMetadata).toSet();
  }

  protected Map<String, String> filterMetadata(AdaptrisMessage msg) {
    MetadataCollection metadata = userMetadataFilter().filter(msg);
    Map<String, String> result = new HashMap<>(metadata.size());
    for (MetadataElement e : metadata) {
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }
}
