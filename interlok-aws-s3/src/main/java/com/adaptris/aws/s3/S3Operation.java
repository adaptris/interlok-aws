package com.adaptris.aws.s3;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.interlok.InterlokException;

public interface S3Operation {

  void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws InterlokException;
  
}
