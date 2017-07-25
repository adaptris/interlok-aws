package com.adaptris.aws.s3;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.http.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.adaptris.interlok.config.DataInputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

/**
 * Abstract base class for S3 Operations.
 * 
 * @author lchan
 *
 */
public abstract class S3OperationImpl implements S3Operation {
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());


  @NotNull
  @Valid
  private DataInputParameter<String> bucketName;
  @NotNull
  @Valid
  private DataInputParameter<String> key;

  @Valid
  @AdvancedConfig
  private MetadataFilter userMetadataFilter;

  public S3OperationImpl() {
  }

  public DataInputParameter<String> getKey() {
    return key;
  }

  public void setKey(DataInputParameter<String> key) {
    this.key = Args.notNull(key, "key");
  }

  public DataInputParameter<String> getBucketName() {
    return bucketName;
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

  protected Map<String, String> filterMetadata(AdaptrisMessage msg) {
    MetadataCollection metadata = userMetadataFilter().filter(msg);
    Map<String, String> result = new HashMap<>(metadata.size());
    for (MetadataElement e : metadata) {
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }

  public void setBucketName(DataInputParameter<String> bucketName) {
    this.bucketName = Args.notNull(bucketName, "bucketName");
  }

  protected TransferManager transferManager(AmazonS3Client s3) {
    return TransferManagerBuilder.standard().withS3Client(s3).build();
  }
}
