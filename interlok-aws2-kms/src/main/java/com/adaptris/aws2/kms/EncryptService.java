package com.adaptris.aws2.kms;

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
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;

import java.nio.ByteBuffer;

/**
 * Encrypt data using AWS KMS
 *
 * @config aws2-kms-encrypt-data
 * @since 4.3.0
 */
@AdapterComponent
@ComponentProfile(summary = "Encrypt data using AWS KMS", recommended = {AWSKMSConnection.class}, since = "4.3.0")
@XStreamAlias("aws2-kms-encrypt-data")
@DisplayOrder(order = {"connection", "keyId", "input", "output", "encryptionAlgorithm", "encryptionContext"})
@NoArgsConstructor
public class EncryptService extends EncryptDecrypt {

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      KmsClient client = awsClient();
      String key = msg.resolve(getKeyId());
      ByteBuffer toBeEncrypted = toByteBuffer(msg);
      EncryptRequest.Builder builder = EncryptRequest.builder();
      builder.keyId(key);
      // Default in the internal SDK is null; so this resolving to null should be OK.
      builder.encryptionAlgorithm(encryptionAlgorithm(msg));
      // Default in the internal SDK has null protection... is defaulting to an empty Map<String,String> a problem?
      builder.encryptionContext(encryptionContext());
      builder.plaintext(SdkBytes.fromByteBuffer(toBeEncrypted));
      EncryptResponse response = client.encrypt(builder.build());
      writeOutput(response.ciphertextBlob().asByteBuffer(), msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

}
