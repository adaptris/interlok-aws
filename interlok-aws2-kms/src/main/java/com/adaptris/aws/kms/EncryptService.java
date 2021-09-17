package com.adaptris.aws.kms;

import java.nio.ByteBuffer;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.util.ExceptionHelper;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.EncryptResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * Encrypt data using AWS KMS
 * 
 * @config aws-kms-encrypt-data
 */
@AdapterComponent
@ComponentProfile(summary = "Encrypt data using AWS KMS", recommended = {AWSKMSConnection.class}, since = "3.10.1")
@XStreamAlias("aws-kms-encrypt-data")
@DisplayOrder(order = {"connection", "keyId", "input", "output", "encryptionAlgorithm", "encryptionContext"})
@NoArgsConstructor
public class EncryptService extends EncryptDecrypt {

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      AWSKMSClient client = awsClient();
      String key = msg.resolve(getKeyId());
      ByteBuffer toBeEncrypted = toByteBuffer(msg);
      EncryptRequest request = new EncryptRequest()
          .withKeyId(key)
          // Default in the internal SDK is null; so this resolving to null should be OK.
          .withEncryptionAlgorithm(encryptionAlgorithm(msg))
          // Default in the internal SDK has null protection... is defaulting to an empty Map<String,String> a problem?
          .withEncryptionContext(encryptionContext())
          .withPlaintext(toBeEncrypted);
      EncryptResult response = client.encrypt(request);
      writeOutput(response.getCiphertextBlob(), msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

}
