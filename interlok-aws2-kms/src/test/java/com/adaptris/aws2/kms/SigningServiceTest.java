package com.adaptris.aws2.kms;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.core.common.MetadataStreamOutput;
import com.adaptris.core.stubs.DefectiveMessageFactory;
import com.adaptris.core.stubs.DefectiveMessageFactory.WhenToBreak;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.util.GuidGenerator;
import com.adaptris.util.text.Base64ByteTranslator;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.MessageType;
import software.amazon.awssdk.services.kms.model.SignRequest;
import software.amazon.awssdk.services.kms.model.SignResponse;
import software.amazon.awssdk.services.kms.model.SigningAlgorithmSpec;

import java.util.Base64;
import java.util.EnumSet;

import static com.adaptris.aws2.kms.LocalstackHelper.HASH_METADATA_KEY;
import static com.adaptris.aws2.kms.LocalstackHelper.MSG_CONTENTS;
import static com.adaptris.aws2.kms.LocalstackHelper.SIG_METADATA_KEY;
import static com.adaptris.aws2.kms.LocalstackHelper.hash;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SigningServiceTest extends ExampleServiceCase {


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

    KmsClient client = Mockito.mock(KmsClient.class);
    SignResponse result = SignResponse.builder().keyId("keyId")
            .signature(SdkBytes.fromByteArray(hash(MSG_CONTENTS)))
            .signingAlgorithm(SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256).build();
    Mockito.when(client.sign((SignRequest)any())).thenReturn(result);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    GenerateSignatureService service = retrieveObjectForSampleConfig().withConnection(connectionMock);

    execute(service, msg);

    assertTrue(msg.headersContainsKey(SIG_METADATA_KEY));

  }

  @Test
  public void testSignExtendedLogging() throws Exception {

    KmsClient client = Mockito.mock(KmsClient.class);
    SignResponse result = SignResponse.builder().keyId("keyId").signature(SdkBytes.fromByteArray(hash(MSG_CONTENTS)))
        .signingAlgorithm(SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256).build();
    Mockito.when(client.sign((SignRequest)any())).thenReturn(result);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    GenerateSignatureService service = retrieveObjectForSampleConfig().withConnection(connectionMock);
    service.setExtendedLogging(true);

    execute(service, msg);

    assertTrue(msg.headersContainsKey(SIG_METADATA_KEY));

  }

  @Test(expected = ServiceException.class)
  public void testSign_Broken() throws Exception {
    KmsClient client = Mockito.mock(KmsClient.class);
    AdaptrisMessage msg =
        new DefectiveMessageFactory(EnumSet.of(WhenToBreak.METADATA_GET)).newMessage(MSG_CONTENTS);
    msg.addMessageHeader(HASH_METADATA_KEY, Base64.getEncoder().encodeToString(hash(MSG_CONTENTS)));

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    GenerateSignatureService service = retrieveObjectForSampleConfig().withConnection(connectionMock);
    execute(service, msg);
  }

}
