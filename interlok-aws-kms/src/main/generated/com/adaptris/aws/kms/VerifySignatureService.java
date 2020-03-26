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
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;
import com.amazonaws.services.kms.model.VerifyRequest;
import com.amazonaws.services.kms.model.VerifyResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Verify a signature using AWS KMS.
 * 
 * @config aws-kms-verify-signature
 */
@AdapterComponent
@ComponentProfile(summary = "Verify a signature using AWS KMS", recommended = {AWSKMSConnection.class})
@XStreamAlias("aws-kms-verify-signature")
@DisplayOrder(order = {"connection", "id", "signatureAlgorithm", "signature", "dataToBeVerified"})
public class VerifySignatureService extends SignatureService {
  /**
   * The source of the signature.
   * 
   * <p>
   * Where the signature is stored; it's probably going to be a {@link ByteArrayFromMetadata} or similar.
   * </p>
   */
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
      VerifyRequest request = new VerifyRequest().withKeyId(key).withSignature(ByteBuffer.wrap(getSignature().wrap(msg))).withMessageType(messageType(msg)).withMessage(ByteBuffer.wrap(getDataToBeVerified().wrap(msg))).withSigningAlgorithm(signingAlgorithm(msg));
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

  /**
   * The source of the signature.
   * 
   * <p>
   * Where the signature is stored; it's probably going to be a {@link ByteArrayFromMetadata} or similar.
   * </p>
   */
  public MessageWrapper<byte[]> getSignature() {
    return this.signature;
  }

  /**
   * The source of the signature.
   * 
   * <p>
   * Where the signature is stored; it's probably going to be a {@link ByteArrayFromMetadata} or similar.
   * </p>
   */
  public void setSignature(final MessageWrapper<byte[]> signature) {
    this.signature = signature;
  }

  /**
   * The data that needs to be checked against the signature.
   * 
   * <p>
   * Since KMS enforces a 4096 byte max on the data that can be used with it; this is likely to be a hash of the payload, so again a
   * {@link ByteArrayFromMetadata} or similar where you have previously generated a hash of the data.
   * </p>
   */
  public MessageWrapper<byte[]> getDataToBeVerified() {
    return this.dataToBeVerified;
  }

  /**
   * The data that needs to be checked against the signature.
   * 
   * <p>
   * Since KMS enforces a 4096 byte max on the data that can be used with it; this is likely to be a hash of the payload, so again a
   * {@link ByteArrayFromMetadata} or similar where you have previously generated a hash of the data.
   * </p>
   */
  public void setDataToBeVerified(final MessageWrapper<byte[]> dataToBeVerified) {
    this.dataToBeVerified = dataToBeVerified;
  }
}
