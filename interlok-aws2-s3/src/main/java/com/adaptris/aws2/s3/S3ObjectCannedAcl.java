package com.adaptris.aws2.s3;

import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

public enum S3ObjectCannedAcl {
  PRIVATE(ObjectCannedACL.PRIVATE),
  PUBLIC_READ(ObjectCannedACL.PUBLIC_READ),
  PUBLIC_READ_WRITE(ObjectCannedACL.PUBLIC_READ_WRITE),
  AUTHENTICATED_READ(ObjectCannedACL.AUTHENTICATED_READ),
//  LOG_DELIVERY_WRITE(CannedAccessControlList.LogDeliveryWrite),
  BUCKET_OWNER_READ(ObjectCannedACL.BUCKET_OWNER_READ),
  BUCKET_OWNER_FULL_CONTROL(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
  AWS_EXEC_READ(ObjectCannedACL.AWS_EXEC_READ);

  private ObjectCannedACL cannedAccessControl;

  S3ObjectCannedAcl(ObjectCannedACL cannedAccessControl){
    this.cannedAccessControl = cannedAccessControl;
  }

  public ObjectCannedACL getCannedAccessControl() {
    return cannedAccessControl;
  }
}
