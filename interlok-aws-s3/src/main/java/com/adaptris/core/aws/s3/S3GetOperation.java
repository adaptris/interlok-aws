package com.adaptris.core.aws.s3;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.common.InputStreamWithEncoding;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.config.DataOutputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Get an object from S3 and store the contents of the object either in the message payload or metadata.
 */
@XStreamAlias("amazon-s3-get")
public class S3GetOperation implements S3Operation {
  
  private transient Logger log = LoggerFactory.getLogger(this.getClass().getName());

  @NotNull
  private DataInputParameter<String> bucketName;

  @NotNull
  private DataInputParameter<String> key;
  
  @NotNull
  private DataOutputParameter<InputStreamWithEncoding> responseBody;

  @Override
  public void execute(AmazonS3Client s3, AdaptrisMessage msg) throws InterlokException {
    GetObjectRequest request = new GetObjectRequest(getBucketName().extract(msg), getKey().extract(msg));
    log.debug("Getting {} from bucket {}", request.getKey(), request.getBucketName());
    S3Object response = s3.getObject(request);
    getResponseBody().insert(new InputStreamWithEncoding(response.getObjectContent(), null), msg);
  }

  public DataInputParameter<String> getKey() {
    return key;
  }

  public void setKey(DataInputParameter<String> key) {
    this.key = key;
  }

  public DataOutputParameter<InputStreamWithEncoding> getResponseBody() {
    return responseBody;
  }

  public void setResponseBody(DataOutputParameter<InputStreamWithEncoding> responseBody) {
    this.responseBody = responseBody;
  }


  public DataInputParameter<String> getBucketName() {
    return bucketName;
  }


  public void setBucketName(DataInputParameter<String> bucketName) {
    this.bucketName = bucketName;
  }
  

}
