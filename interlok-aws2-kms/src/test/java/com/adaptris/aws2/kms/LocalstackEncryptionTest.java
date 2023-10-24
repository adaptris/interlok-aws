package com.adaptris.aws2.kms;

import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.CustomEndpoint;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.common.ByteArrayFromPayload;
import com.adaptris.core.common.PayloadOutputStreamWrapper;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.CustomerMasterKeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.kms.model.OriginType;

import java.nio.charset.StandardCharsets;

import static com.adaptris.aws2.kms.LocalstackHelper.CONFIG;
import static com.adaptris.aws2.kms.LocalstackHelper.KMS_SIGNING_REGION;
import static com.adaptris.aws2.kms.LocalstackHelper.KMS_URL;
import static com.adaptris.aws2.kms.LocalstackHelper.MSG_CONTENTS;
import static com.adaptris.aws2.kms.LocalstackHelper.areTestsEnabled;
import static com.adaptris.aws2.kms.LocalstackHelper.assertEqual;
import static com.adaptris.aws2.kms.LocalstackHelper.assertNotEqual;

// Note that local uses local-kms under the covers
// https://github.com/nsmithuk/local-kms explicitly states that asymmetric operations are not supported.
// So we just test using a symmetric key / default.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackEncryptionTest {


  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
  }

  @Test
  public void test_01_EncryptDecrypt() throws Exception {
    AWSKMSConnection conn = buildConnection();
    try {
      LifecycleHelper.initAndStart(conn);
      String keyId = createSymmetricKey(conn.awsClient());
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS.getBytes(StandardCharsets.UTF_8));

      EncryptService encryptor = generateEncryptService(keyId);
      DecryptService decryptor = generateDecryptService(keyId);

      ExampleServiceCase.execute(encryptor, msg);

      assertNotEqual(MSG_CONTENTS.getBytes(StandardCharsets.UTF_8), msg.getPayload());


      ExampleServiceCase.execute(decryptor, msg);

      assertEqual(MSG_CONTENTS.getBytes(StandardCharsets.UTF_8), msg.getPayload());
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
  }


  protected EncryptService generateEncryptService(String keyId) {
    return new EncryptService()
        .withInput(new ByteArrayFromPayload())
        .withOutput(new PayloadOutputStreamWrapper())
        .withKeyId(keyId)
        .withConnection(buildConnection());
  }

  protected DecryptService generateDecryptService(String keyId) {
    return new DecryptService()
        .withInput(new ByteArrayFromPayload())
        .withOutput(new PayloadOutputStreamWrapper())
        .withKeyId(keyId)
        .withConnection(buildConnection());
  }

  protected AWSKMSConnection buildConnection() {
    String serviceEndpoint = CONFIG.getProperty(KMS_URL);
    String signingRegion = CONFIG.getProperty(KMS_SIGNING_REGION);
    AWSKMSConnection connection = new AWSKMSConnection()
        .withCredentialsProviderBuilder(
                new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("TEST", "TEST")))
        .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint).withSigningRegion(signingRegion));
    return connection;
  }

  private String createSymmetricKey(KmsClient client) throws Exception {
    CreateKeyRequest req = CreateKeyRequest.builder().description("junit")
        .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
        .bypassPolicyLockoutSafetyCheck(true)
        .customerMasterKeySpec(CustomerMasterKeySpec.SYMMETRIC_DEFAULT)
        .origin(OriginType.AWS_KMS).build();
    CreateKeyResponse result = client.createKey(req);
    return result.keyMetadata().keyId();
  }

}
