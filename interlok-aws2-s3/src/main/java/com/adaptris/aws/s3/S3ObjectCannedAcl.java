package com.adaptris.aws.s3;

import com.amazonaws.services.s3.model.CannedAccessControlList;

public enum S3ObjectCannedAcl {
  PRIVATE(CannedAccessControlList.Private),
  PUBLIC_READ(CannedAccessControlList.PublicRead),
  PUBLIC_READ_WRITE(CannedAccessControlList.PublicReadWrite),
  AUTHENTICATED_READ(CannedAccessControlList.AuthenticatedRead),
  LOG_DELIVERY_WRITE(CannedAccessControlList.LogDeliveryWrite),
  BUCKET_OWNER_READ(CannedAccessControlList.BucketOwnerRead),
  BUCKET_OWNER_FULL_CONTROL(CannedAccessControlList.BucketOwnerFullControl),
  AWS_EXEC_READ(CannedAccessControlList.AwsExecRead);

  private CannedAccessControlList cannedAccessControl;

  S3ObjectCannedAcl(CannedAccessControlList cannedAccessControl){
    this.cannedAccessControl = cannedAccessControl;
  }

  public CannedAccessControlList getCannedAccessControl() {
    return cannedAccessControl;
  }
}
