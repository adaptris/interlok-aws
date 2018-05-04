package com.adaptris.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * interface that wraps the amazon s3 client and high level transfer manager instances.
 * 
 *
 */
public interface ClientWrapper {

  public AmazonS3Client amazonClient();

  public TransferManager transferManager();

}
