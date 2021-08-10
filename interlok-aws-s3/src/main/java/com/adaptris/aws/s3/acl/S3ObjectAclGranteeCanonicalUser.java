package com.adaptris.aws.s3.acl;

import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.Owner;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import org.apache.commons.lang3.StringUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents a grantee identified by their canonical Amazon ID.
 */
@NoArgsConstructor
@AllArgsConstructor
@XStreamAlias("s3-object-acl-grantee-canonical-user")
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
    return new CanonicalGrantee(getId());
  }

}
