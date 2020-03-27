package com.adaptris.aws.kms;

import static org.junit.Assert.assertTrue;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.core.common.MetadataStreamOutput;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.PropertyHelper;
import com.adaptris.util.stream.DevNullOutputStream;
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
public class LocalstackServiceTest {

  private static final String TESTS_ENABLED = "localstack.tests.enabled";
  private static final String PROPERTIES_RESOURCE = "unit-tests.properties";
  private static final String KMS_SIGNING_REGION = "localstack.kms.signingRegion";
  private static final String KMS_URL = "localstack.kms.url";

  private static Properties config = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

  public static final String MSG_CONTENTS = "hello world";
  public static final String HASH_METADATA_KEY = "payload-hash";
  public static final String SIG_METADATA_KEY = "payload-signature";

  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
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
      ServiceCase.execute(signer, msg);
      assertTrue(msg.headersContainsKey(SIG_METADATA_KEY));
      
      // Re-hash the payload to "simulate the payload being transported...
      msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));
      VerifySignatureService verify = verifySignatureService(keyId);
      ServiceCase.execute(signer, msg);
      // no exception...
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
  }


  protected static boolean areTestsEnabled() {
    return BooleanUtils.toBoolean(config.getProperty(TESTS_ENABLED, "false"));
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
    String serviceEndpoint = config.getProperty(KMS_URL);
    String signingRegion = config.getProperty(KMS_SIGNING_REGION);
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

  public static byte[] hash(String s) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream in = new ReaderInputStream(new StringReader(s), StandardCharsets.UTF_8);
        DigestOutputStream out = new DigestOutputStream(new DevNullOutputStream(), digest)) {
      IOUtils.copy(in, out);
    }
    return digest.digest();
  }
}
