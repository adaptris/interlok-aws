package com.adaptris.aws.s3;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.http.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.interlok.config.DataInputParameter;

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

  public S3OperationImpl() {
  }

  public DataInputParameter<String> getKey() {
    return key;
  }

  public void setKey(DataInputParameter<String> key) {
    this.key = Args.notNull(key, "key");;
  }

  public DataInputParameter<String> getBucketName() {
    return bucketName;
  }


  public void setBucketName(DataInputParameter<String> bucketName) {
    this.bucketName = Args.notNull(bucketName, "bucketName");
  }


}
