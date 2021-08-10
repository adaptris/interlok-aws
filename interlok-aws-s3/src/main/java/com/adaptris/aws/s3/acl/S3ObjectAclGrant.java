package com.adaptris.aws.s3.acl;


import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Owner;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Specifies a grant, consisting of one grantee and one permission.
 */
@NoArgsConstructor
@AllArgsConstructor
@XStreamAlias("s3-object-grant")
@DisplayOrder(order = {"grantee", "permission"})
public class S3ObjectAclGrant {

  /**
   * The grantee being granted a permission by this grant.
   */
  @Getter
  @Setter
  private S3ObjectAclGrantee grantee;

  /**
   * The permission being granted to the grantee by this grant.
   */
  @Getter
  @Setter
  @InputFieldHint(style = "com.adaptris.aws.s3.S3ObjectAclPermission")
  private S3ObjectAclPermission permission;

  boolean grant(){
    return getGrantee().grant();
  }

  Grant create() {
    return new Grant(getGrantee().create(), getPermission().create());
  }

}
