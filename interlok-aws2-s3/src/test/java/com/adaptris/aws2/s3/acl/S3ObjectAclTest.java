package com.adaptris.aws2.s3.acl;

import org.junit.Test;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Permission;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class S3ObjectAclTest {

  @Test
  public void create() throws Exception {

    List<S3ObjectAclGrant> grants = Arrays.asList(
      new S3ObjectAclGrant(new S3ObjectAclGranteeCanonicalUser("123"), S3ObjectAclPermission.READ),
      new S3ObjectAclGrant(new S3ObjectAclGranteeCanonicalUser(), S3ObjectAclPermission.READ)
    );

    S3ObjectAcl objectAcl = new S3ObjectAcl(grants);

    AccessControlPolicy acl = objectAcl.create();

    List<Grant> resultGrants = acl.grants();
    assertEquals(1, resultGrants.size());

    assertEquals("123", resultGrants.get(0).grantee().id());
    assertEquals(Permission.READ, resultGrants.get(0).permission());

  }

}
