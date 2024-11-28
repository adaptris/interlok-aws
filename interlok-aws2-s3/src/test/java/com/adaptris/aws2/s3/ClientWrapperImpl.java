package com.adaptris.aws2.s3;

import software.amazon.awssdk.services.s3.S3Client;

class ClientWrapperImpl implements ClientWrapper {

  private S3Client client;

  ClientWrapperImpl(S3Client client) {
    this.client = client;
  }
  
  @Override
  public S3Client amazonClient() {
    return client;
  }

}
