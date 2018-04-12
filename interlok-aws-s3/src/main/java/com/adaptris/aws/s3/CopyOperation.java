package com.adaptris.aws.s3;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.http.util.Args;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.types.InterlokMessage;
import com.amazonaws.services.s3.AmazonS3Client;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Copy an object from S3 to another object
 * 
 * <p>
 * Uses {@link AmazonS3Client#copyObject(String, String, String, String)} using only the default behaviour.
 * </p>
 * 
 * @config amazon-s3-copy
 */
@AdapterComponent
@ComponentProfile(summary = "Copy an object in S3 to another Object")
@XStreamAlias("amazon-s3-copy")
@DisplayOrder(order ={ "bucketName", "key", "destinationBucketName", "destinationKey"})
public class CopyOperation extends S3OperationImpl {

  @Valid
  private DataInputParameter<String> destinationBucketName;
  @NotNull
  @Valid
  private DataInputParameter<String> destinationKey;

  public CopyOperation() {
  }

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws InterlokException {
    try {
      AmazonS3Client s3 = wrapper.amazonClient();
      String srcBucket = getBucketName().extract(msg);
      String srcKey = getKey().extract(msg);
      String destBucket = destinationBucket(msg);
      String destKey = getDestinationKey().extract(msg);
      log.trace("Copying [{}:{}] to [{}:{}]", srcBucket, srcKey, destBucket, destKey);
      s3.copyObject(srcBucket, srcKey, destBucket, destKey);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

  public DataInputParameter<String> getDestinationBucketName() {
    return destinationBucketName;
  }

  /**
   * Set the destination bucket.
   * 
   * @param bucket the bucket, if null, then we use {@link #getBucketName()} as the destination.
   */
  public void setDestinationBucketName(DataInputParameter<String> bucket) {
    this.destinationBucketName = bucket;
  }

  String destinationBucket(InterlokMessage msg) throws InterlokException {
    if (getDestinationBucketName() == null) {
      return getBucketName().extract(msg);
    }
    return getDestinationBucketName().extract(msg);
  }

  public DataInputParameter<String> getDestinationKey() {
    return destinationKey;
  }

  /**
   * Set the destination key.
   * 
   * @param key the key.
   */
  public void setDestinationKey(DataInputParameter<String> key) {
    this.destinationKey = Args.notNull(key, "destinationKey");
  }

}
