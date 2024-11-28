package com.adaptris.aws2.kms;

import static com.adaptris.aws2.kms.LocalstackHelper.MSG_CONTENTS;
import static com.adaptris.aws2.kms.LocalstackHelper.hash;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ByteArrayFromPayload;
import com.adaptris.core.common.PayloadOutputStreamWrapper;
import com.adaptris.core.stubs.DefectiveMessageFactory;
import com.adaptris.core.stubs.DefectiveMessageFactory.WhenToBreak;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.util.GuidGenerator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;

public class EncryptServiceTest extends ExampleServiceCase {

  @Override
  protected EncryptService retrieveObjectForSampleConfig() {
    return new EncryptService()
        .withInput(new ByteArrayFromPayload())
        .withOutput(new PayloadOutputStreamWrapper())
        // It's a UUID not an alias.
        .withKeyId(new GuidGenerator().getUUID())
        .withConnection(new AWSKMSConnection());
  }


  @Test
  public void testEncrypt() throws Exception {
    KmsClient client = Mockito.mock(KmsClient.class);
    EncryptResponse result = EncryptResponse.builder().keyId("keyId").ciphertextBlob(SdkBytes.fromByteArray(hash(MSG_CONTENTS))).build();

    Mockito.when(client.encrypt((EncryptRequest)any())).thenReturn(result);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS);

    EncryptService service =
        retrieveObjectForSampleConfig().withEncryptionAlgorithm("SYMMETRIC_DEFAULT").withConnection(connectionMock);

    execute(service, msg);

    assertNotNull(msg.getPayload());
    assertNotEquals(MSG_CONTENTS, msg.getContent());
  }

  @Test
  public void testSign_Broken() throws Exception {
    Assertions.assertThrows(ServiceException.class, () -> {
      KmsClient client = Mockito.mock(KmsClient.class);

      AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
      Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
      when(connectionMock.awsClient()).thenReturn(client);

      AdaptrisMessage msg = new DefectiveMessageFactory(EnumSet.of(WhenToBreak.METADATA_GET)).newMessage(MSG_CONTENTS);

      EncryptService service = retrieveObjectForSampleConfig().withEncryptionContext(null).withConnection(connectionMock);
      execute(service, msg);
    });
  }


}
