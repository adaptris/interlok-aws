package com.adaptris.aws2.s3.acl;

import software.amazon.awssdk.services.s3.model.Grantee;

public interface S3ObjectAclGrantee {

  boolean grant();

  Grantee create();

}
