package com.adaptris.aws.kms;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

import java.nio.ByteBuffer;

/**
 * Decrypt data using AWS KMS
 * 
 * @config aws-kms-decrypt-data
 */
@AdapterComponent
@ComponentProfile(summary = "Decrypt data using AWS KMS", recommended = {AWSKMSConnection.class}, since = "3.10.1")
@XStreamAlias("aws-kms-decrypt-data")
@DisplayOrder(order = {"connection", "keyId", "input", "output", "encryptionAlgorithm", "encryptionContext"})
@NoArgsConstructor
public class DecryptService extends EncryptDecrypt {

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      KmsClient client = awsClient();
      String key = msg.resolve(getKeyId());
      ByteBuffer toBeDecrypted = toByteBuffer(msg);
      DecryptRequest.Builder builder = DecryptRequest.builder();
      builder.keyId(key);
      // Default in the internal SDK is null; so this resolving to null should be OK.
      builder.encryptionAlgorithm(encryptionAlgorithm(msg));
      // Default in the internal SDK has null protection... is defaulting to an empty Map<String,String> a problem?
      builder.encryptionContext(encryptionContext());
      builder.ciphertextBlob(SdkBytes.fromByteBuffer(toBeDecrypted));

      DecryptResponse response = client.decrypt(builder.build());
      writeOutput(response.plaintext().asByteBuffer(), msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

}
