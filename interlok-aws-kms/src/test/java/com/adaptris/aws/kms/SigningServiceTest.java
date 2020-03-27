package com.adaptris.aws.kms;

import static com.adaptris.aws.kms.LocalstackServiceTest.HASH_METADATA_KEY;
import static com.adaptris.aws.kms.LocalstackServiceTest.MSG_CONTENTS;
import static com.adaptris.aws.kms.LocalstackServiceTest.SIG_METADATA_KEY;
import static com.adaptris.aws.kms.LocalstackServiceTest.hash;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.EnumSet;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.aws.kms.GenerateSignatureService.ByteBufferInputStream;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.core.common.MetadataStreamOutput;
import com.adaptris.core.stubs.DefectiveMessageFactory;
import com.adaptris.core.stubs.DefectiveMessageFactory.WhenToBreak;
import com.adaptris.util.GuidGenerator;
import com.adaptris.util.text.Base64ByteTranslator;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SignResult;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;

public class SigningServiceTest extends ServiceCase {

  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

  @Override
  protected GenerateSignatureService retrieveObjectForSampleConfig() {
    return new GenerateSignatureService()
        .withInput(new ByteArrayFromMetadata().withKey(HASH_METADATA_KEY).withTranslator(new Base64ByteTranslator()))
        .withOutput(new MetadataStreamOutput().withMetadataKey(SIG_METADATA_KEY).withTranslator(new Base64ByteTranslator()))
        .withMessageType(MessageType.DIGEST.name())
        .withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_512.name())
        // It's a UUID not an alias.
        .withKeyId(new GuidGenerator().getUUID())
        .withConnection(new AWSKMSConnection());
  }


  @Test
  public void testSign() throws Exception {
    AWSKMSClient client = Mockito.mock(AWSKMSClient.class);
    SignResult result = new SignResult().withKeyId("keyId").withSignature(ByteBuffer.wrap(hash(MSG_CONTENTS)))
        .withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256);
    Mockito.when(client.sign(any())).thenReturn(result);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    GenerateSignatureService service = retrieveObjectForSampleConfig().withConnection(new MockKmsConnection(client));

    execute(service, msg);

    assertTrue(msg.headersContainsKey(SIG_METADATA_KEY));

  }

  @Test(expected = ServiceException.class)
  public void testSign_Broken() throws Exception {
    AWSKMSClient client = Mockito.mock(AWSKMSClient.class);
    AdaptrisMessage msg =
        new DefectiveMessageFactory(EnumSet.of(WhenToBreak.METADATA_GET)).newMessage(MSG_CONTENTS);
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    GenerateSignatureService service = retrieveObjectForSampleConfig().withConnection(new MockKmsConnection(client));
    execute(service, msg);
  }

  @Test
  public void testByteBufferInputStream() throws Exception {
    ByteBuffer buf = ByteBuffer.wrap(MSG_CONTENTS.getBytes(StandardCharsets.UTF_8));
    try (ByteBufferInputStream in = new ByteBufferInputStream(buf)) {
      int i = in.read();
      while (i != -1) {
        i = in.read();
      }
    }
    try (ByteBufferInputStream in = new ByteBufferInputStream(buf)) {
      byte[] bytes = IOUtils.toByteArray(in);
    }
  }
}
