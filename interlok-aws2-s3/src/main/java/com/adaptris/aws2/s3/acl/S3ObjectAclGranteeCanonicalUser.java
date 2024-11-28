package com.adaptris.aws2.s3.acl;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.model.Grantee;

/**
 * Represents a grantee identified by their canonical Amazon ID.
 */
@NoArgsConstructor
@AllArgsConstructor
@XStreamAlias("aws2-s3-object-acl-grantee-canonical-user")
public class S3ObjectAclGranteeCanonicalUser implements S3ObjectAclGrantee {

  /**
   * Represents a grantee identified by their canonical Amazon ID.
   */
  @Getter
  @Setter
  private String id;

  @Override
  public boolean grant() {
    return StringUtils.isNotEmpty(getId());
  }

  @Override
  public Grantee create() {
    Grantee.Builder builder = Grantee.builder();
    builder.id(getId());
    return builder.build();
  }

}
