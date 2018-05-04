package com.adaptris.aws.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Delete an object from S3.
 * 
 * @author lchan
 * @config amazon-s3-download
 */
@AdapterComponent
@ComponentProfile(summary = "Delete an object from S3")
@XStreamAlias("amazon-s3-delete")
@DisplayOrder(order ={ "bucketName", "key"})
public class DeleteOperation extends S3OperationImpl {

  public DeleteOperation() {
  }

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws InterlokException {
    try {
      AmazonS3Client s3 = wrapper.amazonClient();
      String bucket = getBucketName().extract(msg);
      String key = getKey().extract(msg);
      log.trace("Deleting [{}:{}]", bucket, key);
      s3.deleteObject(bucket, key);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

}
