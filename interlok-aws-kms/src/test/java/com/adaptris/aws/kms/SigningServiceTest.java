package com.adaptris.aws.kms;

import com.adaptris.core.ServiceCase;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.core.common.MetadataStreamOutput;
import com.adaptris.util.text.Base64ByteTranslator;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;

public class SigningServiceTest extends ServiceCase {

  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new GenerateSignatureService()
        .withInput(new ByteArrayFromMetadata().withKey("payload-hash").withTranslator(new Base64ByteTranslator()))
        .withOutput(new MetadataStreamOutput().withMetadataKey("payloadSignature").withTranslator(new Base64ByteTranslator()))
        .withMessageType(MessageType.DIGEST.name())
        .withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_512.name())
        .withKeyId("My Key Alias")
        .withConnection(new AWSKMSConnection());
  }

}
