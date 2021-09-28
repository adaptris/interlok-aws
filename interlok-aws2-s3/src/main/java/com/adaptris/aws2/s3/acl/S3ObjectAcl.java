package com.adaptris.aws2.s3.acl;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections4.ListUtils;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;

import java.util.List;

/**
 * Represents an Amazon S3 Access Control List (ACL), including the ACL's set of
 * grantees and the permissions assigned to each grantee.
 */
@NoArgsConstructor
@AllArgsConstructor
@XStreamAlias("s3-object-acl")
public class S3ObjectAcl {

  @Getter
  @Setter
  @XStreamImplicit
  private List<S3ObjectAclGrant> grants;

  public AccessControlPolicy create() {
    AccessControlPolicy.Builder builder = AccessControlPolicy.builder();

    for (S3ObjectAclGrant grant : ListUtils.emptyIfNull(getGrants())) {
      if(grant.grant()){
        builder.grants(grant.create());
      }
    }

    return builder.build();
  }

}
