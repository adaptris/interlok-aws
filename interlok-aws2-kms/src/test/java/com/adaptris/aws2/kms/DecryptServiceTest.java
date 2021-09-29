package com.adaptris.aws2.kms;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ByteArrayFromPayload;
import com.adaptris.core.common.PayloadOutputStreamWrapper;
import com.adaptris.core.stubs.DefectiveMessageFactory;
import com.adaptris.core.stubs.DefectiveMessageFactory.WhenToBreak;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.util.GuidGenerator;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

import java.util.EnumSet;

import static com.adaptris.aws2.kms.LocalstackHelper.MSG_CONTENTS;
import static com.adaptris.aws2.kms.LocalstackHelper.hash;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DecryptServiceTest extends ExampleServiceCase {

  @Override
  protected DecryptService retrieveObjectForSampleConfig() {
    return new DecryptService()
        .withInput(new ByteArrayFromPayload())
        .withOutput(new PayloadOutputStreamWrapper())
        // It's a UUID not an alias.
        .withKeyId(new GuidGenerator().getUUID())
        .withConnection(new AWSKMSConnection());
  }


  @Test
  public void testEncrypt() throws Exception {
    KmsClient client = Mockito.mock(KmsClient.class);
    DecryptResponse result = DecryptResponse.builder().keyId("keyId").plaintext(SdkBytes.fromByteArray(hash(MSG_CONTENTS))).build();

    Mockito.when(client.decrypt((DecryptRequest)any())).thenReturn(result);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS);

    DecryptService service =
        retrieveObjectForSampleConfig().withConnection(connectionMock);

    execute(service, msg);

    assertNotNull(msg.getPayload());
    assertNotEquals(MSG_CONTENTS, msg.getContent());
  }

  @Test(expected = ServiceException.class)
  public void testSign_Broken() throws Exception {
    KmsClient client = Mockito.mock(KmsClient.class);

    AWSKMSConnection connectionMock = mock(AWSKMSConnection.class);
    Mockito.when(connectionMock.retrieveConnection(AWSKMSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.awsClient()).thenReturn(client);

    AdaptrisMessage msg = new DefectiveMessageFactory(EnumSet.of(WhenToBreak.METADATA_GET)).newMessage(MSG_CONTENTS);

    DecryptService service = retrieveObjectForSampleConfig().withEncryptionContext(null).withConnection(connectionMock);
    execute(service, msg);
  }


}
