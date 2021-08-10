package com.adaptris.aws.s3.acl;

import java.util.List;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Owner;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

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

  public AccessControlList create() {
    AccessControlList objectAcl = new AccessControlList();


    for (S3ObjectAclGrant grant : getGrants()){
      if(grant.grant()){
        objectAcl.grantAllPermissions(grant.create());
      }
    }

    return objectAcl;
  }

}
