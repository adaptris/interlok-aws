package com.adaptris.aws.kms;

import com.adaptris.core.ServiceCase;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.util.text.Base64ByteTranslator;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;

public class VerifyServiceTest extends ServiceCase {

  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new VerifySignatureService()
        .withDataToBeVerified(new ByteArrayFromMetadata().withKey("payload-hash").withTranslator(new Base64ByteTranslator()))
        .withSignature(new ByteArrayFromMetadata().withKey("signature-base64").withTranslator(new Base64ByteTranslator()))
        .withMessageType(MessageType.DIGEST.name())
        .withSigningAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_512.name())
        .withKeyId("My Key Alias")
        .withConnection(new AWSKMSConnection());
  }

}
