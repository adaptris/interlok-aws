package com.adaptris.aws2.kms;

import static com.adaptris.aws2.kms.LocalstackHelper.CONFIG;
import static com.adaptris.aws2.kms.LocalstackHelper.HASH_METADATA_KEY;
import static com.adaptris.aws2.kms.LocalstackHelper.KMS_SIGNING_REGION;
import static com.adaptris.aws2.kms.LocalstackHelper.KMS_URL;
import static com.adaptris.aws2.kms.LocalstackHelper.MSG_CONTENTS;
import static com.adaptris.aws2.kms.LocalstackHelper.SIG_METADATA_KEY;
import static com.adaptris.aws2.kms.LocalstackHelper.hash;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.CustomEndpoint;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.core.common.MetadataStreamOutput;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.util.text.Base64ByteTranslator;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.CreateKeyRequest;
import com.amazonaws.services.kms.model.CreateKeyResult;
import com.amazonaws.services.kms.model.CustomerMasterKeySpec;
import com.amazonaws.services.kms.model.KeyUsageType;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.OriginType;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;

// Note that local uses local-kms under the covers
// https://github.com/nsmithuk/local-kms explicitly states that asymmetric operations are not supported.
// So this conceptually is correct; but it won't work.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackSigningTest {

  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(false);
    // Assume.assumeTrue(areTestsEnabled());
  }

  @Test
  public void test_01_SignVerify() throws Exception {
    AWSKMSConnection conn = buildConnection();
    try {
      LifecycleHelper.initAndStart(conn);
      String keyId = createMasterSigningKey(conn.awsClient());
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS, StandardCharsets.UTF_8.name());

      msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

      // Now create the signature
      GenerateSignatureService signer = generateSignatureService(keyId);
      ExampleServiceCase.execute(signer, msg);
      assertTrue(msg.headersContainsKey(SIG_METADATA_KEY));

      // Re-hash the payload to "simulate the payload being transported...
      msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));
      VerifySignatureService verify = verifySignatureService(keyId);
      ExampleServiceCase.execute(signer, msg);
      // no exception...
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
  }

  protected GenerateSignatureService generateSignatureService(String keyId) {
    return new GenerateSignatureService()
    .withInput(new ByteArrayFromMetadata().withKey(HASH_METADATA_KEY).withTranslator(new Base64ByteTranslator()))
    .withOutput(new MetadataStreamOutput().withMetadataKey(SIG_METADATA_KEY).withTranslator(new Base64ByteTranslator()))
    .withMessageType(MessageType.DIGEST.name())
    .withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_512.name())
    .withKeyId(keyId)
    .withConnection(buildConnection());
  }


  protected VerifySignatureService verifySignatureService(String keyId) {
    return new VerifySignatureService()
        .withDataToBeVerified(new ByteArrayFromMetadata().withKey(HASH_METADATA_KEY).withTranslator(new Base64ByteTranslator()))
        .withSignature(new ByteArrayFromMetadata().withKey(SIG_METADATA_KEY).withTranslator(new Base64ByteTranslator()))
        .withMessageType(MessageType.DIGEST.name()).withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_512.name())
        .withKeyId(keyId).withConnection(buildConnection());
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

  private String createMasterSigningKey(AWSKMSClient client) throws Exception {
    CreateKeyRequest req = new CreateKeyRequest().withDescription("junit")
        .withKeyUsage(KeyUsageType.SIGN_VERIFY)
        .withBypassPolicyLockoutSafetyCheck(true)
        .withCustomerMasterKeySpec(CustomerMasterKeySpec.RSA_2048)
        .withOrigin(OriginType.AWS_KMS);
    CreateKeyResult result = client.createKey(req);
    System.err.println(result.getKeyMetadata().getSigningAlgorithms());
    return result.getKeyMetadata().getKeyId();
  }

}
