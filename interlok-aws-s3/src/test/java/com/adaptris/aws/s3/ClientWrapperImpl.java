package com.adaptris.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;

class ClientWrapperImpl implements ClientWrapper {

  private AmazonS3Client client;
  private TransferManager transferManager;
  
  ClientWrapperImpl(AmazonS3Client client) {
    this(client, null);
  }
  
  ClientWrapperImpl(AmazonS3Client client, TransferManager transferManager) {
    this.client = client;
    this.transferManager = transferManager;
  }
  
  @Override
  public AmazonS3Client amazonClient() {
    return client;
  }

  @Override
  public TransferManager transferManager() {
    return transferManager;
  }

}
