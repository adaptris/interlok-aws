package com.adaptris.aws.s3.acl;

import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.Owner;

public interface S3ObjectAclGrantee {

  boolean grant();

  Grantee create();

}
