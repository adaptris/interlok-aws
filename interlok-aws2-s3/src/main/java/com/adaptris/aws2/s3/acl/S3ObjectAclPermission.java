package com.adaptris.aws2.s3.acl;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.services.s3.model.Permission;

/**
 * Specifies constants defining an access permission.
 */
@AllArgsConstructor
public enum S3ObjectAclPermission {

  FULL_CONTROL(Permission.FULL_CONTROL),
  READ(Permission.READ),
  READ_ACP(Permission.READ_ACP),
  WRITE(Permission.WRITE),
  WRITE_ACP(Permission.WRITE_ACP);

  private Permission permission;

  Permission create(){
    return permission;
  }

}
