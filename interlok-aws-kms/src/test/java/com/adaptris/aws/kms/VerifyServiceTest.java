package com.adaptris.aws.kms;

import static com.adaptris.aws.kms.LocalstackHelper.HASH_METADATA_KEY;
import static com.adaptris.aws.kms.LocalstackHelper.MSG_CONTENTS;
import static com.adaptris.aws.kms.LocalstackHelper.SIG_METADATA_KEY;
import static com.adaptris.aws.kms.LocalstackHelper.hash;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Base64;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.core.stubs.DefectiveMessageFactory;
import com.adaptris.core.stubs.DefectiveMessageFactory.WhenToBreak;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.util.GuidGenerator;
import com.adaptris.util.text.Base64ByteTranslator;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;
import com.amazonaws.services.kms.model.VerifyResult;

public class VerifyServiceTest extends ExampleServiceCase {

  @Override
  protected VerifySignatureService retrieveObjectForSampleConfig() {
    return new VerifySignatureService()
        .withDataToBeVerified(new ByteArrayFromMetadata().withKey(HASH_METADATA_KEY).withTranslator(new Base64ByteTranslator()))
        .withSignature(new ByteArrayFromMetadata().withKey(SIG_METADATA_KEY).withTranslator(new Base64ByteTranslator()))
        .withMessageType(MessageType.DIGEST.name())
        .withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_512.name())
        // It's a UUID not an alias.
        .withKeyId(new GuidGenerator().getUUID())
        .withConnection(new AWSKMSConnection());
  }


  @Test
  public void testVerify() throws Exception {
    AWSKMSClient client = Mockito.mock(AWSKMSClient.class);
    VerifyResult result = new VerifyResult().withKeyId("keyId")
        .withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256)
        .withSignatureValid(true);
    Mockito.when(client.verify(any())).thenReturn(result);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    VerifySignatureService service = retrieveObjectForSampleConfig().withConnection(connectionMock);

    execute(service, msg);

  }

  @Test
  public void testVerifyExtendedLogging() throws Exception {
    AWSKMSClient client = Mockito.mock(AWSKMSClient.class);
    VerifyResult result = new VerifyResult().withKeyId("keyId").withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256)
        .withSignatureValid(true);
    Mockito.when(client.verify(any())).thenReturn(result);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    VerifySignatureService service = retrieveObjectForSampleConfig().withConnection(connectionMock);
    service.setExtendedLogging(true);
    execute(service, msg);

  }

  @Test
  public void testSign_InvalidSignature() throws Exception {
    AWSKMSClient client = Mockito.mock(AWSKMSClient.class);
    VerifyResult result = new VerifyResult().withKeyId("keyId")
        .withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256)
        .withSignatureValid(false);
    Mockito.when(client.verify(any())).thenReturn(result);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);


    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    VerifySignatureService service = retrieveObjectForSampleConfig().withConnection(connectionMock);
    assertThrows(ServiceException.class, ()->{
      execute(service, msg);
    }, "Failed, invalid signature");
  }

  @Test
  public void testVerify_Broken() throws Exception {
    AWSKMSClient client = Mockito.mock(AWSKMSClient.class);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    AdaptrisMessage msg = new DefectiveMessageFactory(EnumSet.of(WhenToBreak.METADATA_GET)).newMessage(MSG_CONTENTS);
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    VerifySignatureService service = retrieveObjectForSampleConfig().withConnection(connectionMock);
    assertThrows(ServiceException.class, ()->{
      execute(service, msg);
    }, "Failed, verify broken");
  }


}
