package com.adaptris.aws.s3.acl;

import com.amazonaws.services.s3.model.Grantee;

public interface S3ObjectAclGrantee {

  boolean grant();

  Grantee create();

}
