package com.adaptris.aws.s3;

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.common.InputStreamWithEncoding;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataOutputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Get an object from S3 and store the contents of the object either in the message payload or metadata.
 * 
 * @author gdries
 * @config amazon-s3-get
 */
@XStreamAlias("amazon-s3-get")
@DisplayOrder(order = {"key", "bucketName", "responseBody", "userMetadataFilter"})
public class S3GetOperation extends S3OperationImpl {

  @NotNull
  private DataOutputParameter<InputStreamWithEncoding> responseBody;


  @Override
  public void execute(AmazonS3Client s3, AdaptrisMessage msg) throws InterlokException {
    GetObjectRequest request = new GetObjectRequest(getBucketName().extract(msg), getKey().extract(msg));

    log.debug("Getting {} from bucket {}", request.getKey(), request.getBucketName());
    S3Object response = s3.getObject(request);
    log.trace("Object is {} bytes", response.getObjectMetadata().getContentLength());
    getResponseBody().insert(new InputStreamWithEncoding(response.getObjectContent(), null), msg);
    msg.setMetadata(filterUserMetadata(response.getObjectMetadata().getUserMetadata()));
  }

  public DataOutputParameter<InputStreamWithEncoding> getResponseBody() {
    return responseBody;
  }

  public void setResponseBody(DataOutputParameter<InputStreamWithEncoding> responseBody) {
    this.responseBody = responseBody;
  }
}
