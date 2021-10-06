package com.adaptris.aws.s3.acl;

import com.amazonaws.services.s3.model.Permission;

import lombok.AllArgsConstructor;

/**
 * Specifies constants defining an access permission.
 */
@AllArgsConstructor
public enum S3ObjectAclPermission {

  FULL_CONTROL(Permission.FullControl),
  READ(Permission.Read),
  READ_ACP(Permission.ReadAcp),
  WRITE(Permission.Write),
  WRITE_ACP(Permission.WriteAcp);

  private Permission permission;

  Permission create(){
    return permission;
  }

}
