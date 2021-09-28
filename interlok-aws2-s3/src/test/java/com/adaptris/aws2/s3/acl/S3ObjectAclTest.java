package com.adaptris.aws2.s3.acl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Permission;

import org.junit.Test;

public class S3ObjectAclTest {

  @Test
  public void create() throws Exception {

    List<S3ObjectAclGrant> grants = Arrays.asList(
      new S3ObjectAclGrant(new S3ObjectAclGranteeCanonicalUser("123"), S3ObjectAclPermission.READ),
      new S3ObjectAclGrant(new S3ObjectAclGranteeCanonicalUser(), S3ObjectAclPermission.READ)
    );

    S3ObjectAcl objectAcl = new S3ObjectAcl(grants);

    AccessControlList acl = objectAcl.create();

    List<Grant> resultGrants = acl.getGrantsAsList();
    assertEquals(1, resultGrants.size());

    assertEquals("123", resultGrants.get(0).getGrantee().getIdentifier());
    assertEquals(Permission.Read, resultGrants.get(0).getPermission());

  }

}
