package com.adaptris.aws2.s3.acl;


import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.services.s3.model.Grant;

/**
 * Specifies a grant, consisting of one grantee and one permission.
 */
@NoArgsConstructor
@AllArgsConstructor
@XStreamAlias("aws2-s3-object-grant")
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
   * <ul>
   * <li><b>FULL_CONTROL:</b> Allows grantee the READ, READ_ACP, and WRITE_ACP permissions on the object.</li>
   * <li><b>READ:</b> Allows grantee to read the object data and its metadata.</li>
   * <li><b>READ_ACP:</b> Allows grantee to read the object ACL.</li>
   * <li><b>WRITE:</b> Not applicable for objects, only for buckets.</li>
   * <li><b>WRITE_ACP:</b> Allows grantee to write the ACL for the applicable object.</li>
   * </ul>
   */
  @Getter
  @Setter
  @InputFieldHint(style = "com.adaptris.aws2.s3.S3ObjectAclPermission")
  private S3ObjectAclPermission permission;

  boolean grant(){
    return getGrantee().grant();
  }

  Grant create() {
    Grant.Builder builder = Grant.builder();
    builder.grantee(grantee.create());
    builder.permission(permission.create());
    return builder.build();
  }

}
