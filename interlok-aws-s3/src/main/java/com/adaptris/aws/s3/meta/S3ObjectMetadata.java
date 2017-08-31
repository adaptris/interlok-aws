package com.adaptris.aws.s3.meta;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * ObjectMetadata is the base class for all the different ObjectMetadata an S3 
 * upload operation can have. Any settings added by the user will be apply()d to 
 * the ObjectMetadata before the S3 object is uploaded.
 */
public abstract class S3ObjectMetadata implements Comparable<S3ObjectMetadata> {
  
  public abstract void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException;

  @Override
  public final int compareTo(S3ObjectMetadata o) {
    return getClass().getName().compareTo(o.getClass().getName());
  }
  
}
