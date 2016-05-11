package com.adaptris.core.aws.s3;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.interlok.InterlokException;
import com.amazonaws.services.s3.AmazonS3Client;

public interface S3Operation {

  public abstract void execute(AmazonS3Client s3, AdaptrisMessage msg) throws InterlokException;
  
}
