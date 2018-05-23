package com.adaptris.aws.s3;

import java.util.*;

import javax.validation.Valid;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.amazonaws.services.s3.model.Tag;

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

  MetadataFilter userMetadataFilter() {
    return getUserMetadataFilter() != null ? getUserMetadataFilter() : new RemoveAllMetadataFilter();
  }

  protected Set<MetadataElement> filterUserMetadata(Map<String, String> userMetadata) {
    MetadataCollection msgMetadata = new MetadataCollection();
    for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
      msgMetadata.add(new MetadataElement(entry.getKey(), entry.getValue()));
    }
    return userMetadataFilter().filter(msgMetadata).toSet();
  }

  protected Map<String, String> filterUserMetadata(AdaptrisMessage msg) {
    MetadataCollection metadata = userMetadataFilter().filter(msg);
    Map<String, String> result = new HashMap<>(metadata.size());
    for (MetadataElement e : metadata) {
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }
}
