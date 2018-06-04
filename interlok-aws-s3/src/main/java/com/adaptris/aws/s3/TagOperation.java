package com.adaptris.aws.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.types.InterlokMessage;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.http.util.Args;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

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
@DisplayOrder(order ={ "bucketName", "key", "tagMetadataFilter"})
public class TagOperation extends S3OperationImpl {

  @Valid
  private MetadataFilter tagMetadataFilter;

  public TagOperation() {
  }

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws InterlokException {
    try {
      AmazonS3Client s3 = wrapper.amazonClient();
      String srcBucket = getBucketName().extract(msg);
      String srcKey = getKey().extract(msg);
      List<Tag> tags = filterTagMetadata(msg);
      if(!tags.isEmpty()) {
        log.trace("Tagging [{}:{}]", srcBucket, srcKey);
        s3.setObjectTagging(new SetObjectTaggingRequest(srcBucket, srcKey, new ObjectTagging(tags)));
      }
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
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
    this.tagMetadataFilter = mf;
  }

  private MetadataFilter tagMetadataFilter() {
    return getTagMetadataFilter() != null ? getTagMetadataFilter() : new RemoveAllMetadataFilter();
  }

  private List<Tag> filterTagMetadata(AdaptrisMessage msg) {
    MetadataCollection metadata = tagMetadataFilter().filter(msg);
    List<Tag> result = new ArrayList<>(metadata.size());
    for (MetadataElement e : metadata) {
      result.add(new Tag(e.getKey(), e.getValue()));
    }
    return result;
  }

}
