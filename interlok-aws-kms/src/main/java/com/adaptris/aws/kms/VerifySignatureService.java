package com.adaptris.aws.kms;

import java.nio.ByteBuffer;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.types.MessageWrapper;
import com.adaptris.interlok.util.Args;
import com.adaptris.util.text.HexDump;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;
import com.amazonaws.services.kms.model.VerifyRequest;
import com.amazonaws.services.kms.model.VerifyResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Verify a signature using AWS KMS.
 * 
 * <p>
 * If the signature does not verify for any reason then a normal {@link ServiceException} will be thrown.
 * </p>
 * 
 * @config aws-kms-verify-signature
 */
@AdapterComponent
@ComponentProfile(summary = "Verify a signature using AWS KMS", recommended = {AWSKMSConnection.class}, since = "3.10.1")
@XStreamAlias("aws-kms-verify-signature")
@DisplayOrder(order = {"connection", "keyId", "signingAlgorithm", "messageType", "signature", "dataToBeVerified"})
@NoArgsConstructor
public class VerifySignatureService extends SignatureService {

  /**
   * The source of the signature.
   * 
   * <p>
   * Where the signature is stored; it's probably going to be a {@link ByteArrayFromMetadata} or similar.
   * </p>
   */
  @Getter
  @Setter
  @NotNull
  @Valid
  private MessageWrapper<byte[]> signature;
  /**
   * The data that needs to be checked against the signature.
   * 
   * <p>
   * Since KMS enforces a 4096 byte max on the data that can be used with it; this is likely to be a hash of the payload, so again a
   * {@link ByteArrayFromMetadata} or similar where you have previously generated a hash of the data.
   * </p>
   */
  @Getter
  @Setter
  @NotNull
  @Valid
  private MessageWrapper<byte[]> dataToBeVerified;

  @Override
  public void prepare() throws CoreException {
    Args.notNull(getSignature(), "signature");
    Args.notNull(getDataToBeVerified(), "data-to-be-verified");
    super.prepare();
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      AWSKMSClient client = awsClient();
      String key = msg.resolve(getKeyId());
      SigningAlgorithmSpec signingAlg = signingAlgorithm(msg);
      MessageType type = messageType(msg);
      byte[] signature = getSignature().wrap(msg);
      byte[] toVerify = getDataToBeVerified().wrap(msg);

      if (extendedLogging()) {
        log.trace("Data:\n{}", HexDump.parse(toVerify));
        log.trace("Signature:\n{}", HexDump.parse(signature));
      }
      VerifyRequest request = new VerifyRequest()
              .withKeyId(key)
              .withSignature(ByteBuffer.wrap(signature))
              .withMessageType(messageType(msg))
              .withMessage(ByteBuffer.wrap(toVerify))
              .withSigningAlgorithm(signingAlgorithm(msg));
      
      VerifyResult response = client.verify(request);
      if (!response.isSignatureValid()) {
        throw new ServiceException("Signature was not verified; VerifyResult#isSignatureValid() is false");
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }


  public VerifySignatureService withSignature(MessageWrapper<byte[]> w) {
    setSignature(w);
    return this;
  }

  public VerifySignatureService withDataToBeVerified(MessageWrapper<byte[]> w) {
    setDataToBeVerified(w);
    return this;
  }
}
