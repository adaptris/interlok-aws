package com.adaptris.aws.s3;

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.interlok.InterlokException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@ComponentProfile(summary="Amazon S3 Service")
@XStreamAlias("amazon-s3-service")
public class S3Service extends ServiceImp {

  private transient AmazonS3Client s3;

  @NotNull
  private S3Operation operation;
  
  public S3Service() {
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      getOperation().execute(s3, msg);
    } catch (InterlokException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public void prepare() throws CoreException {
  }

  @Override
  protected void closeService() {
  }

  @Override
  protected void initService() throws CoreException {
    if(getOperation() == null) {
      throw new ServiceException("No operation configured");
    }
    
    s3 = new AmazonS3Client();
  }

  public S3Operation getOperation() {
    return operation;
  }

  public void setOperation(S3Operation operation) {
    this.operation = operation;
  }

}
