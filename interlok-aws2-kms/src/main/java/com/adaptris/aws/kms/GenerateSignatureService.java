package com.adaptris.aws.kms;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.apache.commons.io.IOUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ByteArrayFromMetadata;
import com.adaptris.core.common.MetadataStreamOutput;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.types.MessageWrapper;
import com.adaptris.interlok.util.Args;
import com.adaptris.util.text.HexDump;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SignRequest;
import com.amazonaws.services.kms.model.SignResult;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Generate a signature using AWS KMS.
 * 
 * @config aws-kms-signing
 */
@AdapterComponent
@ComponentProfile(summary = "Signing via AWS KMS", recommended = {AWSKMSConnection.class}, since = "3.10.1")
@XStreamAlias("aws-kms-generate-signature")
@DisplayOrder(order = {"connection", "keyId", "signingAlgorithm", "messageType", "input", "output"})
@NoArgsConstructor
public class GenerateSignatureService extends SignatureService {

  /**
   * The data to be signed.
   * <p>
   * Since KMS enforces a 4096 byte max on the data that can be used with it; this is likely to be a hash of the payload, so again a
   * {@link ByteArrayFromMetadata} or similar where you have previously generated a hash of the data.
   * </p>
   */
  @Getter
  @Setter
  @NotNull
  @Valid
  private MessageWrapper<byte[]> input;

  /**
   * Where to store the signature that was generated.
   * <p>
   * Since the signature will be an opaque set of bytes; you probably want to use {@link MetadataStreamOutput} or similar to encode
   * the signature before storing it as metadata.
   * </p>
   */
  @Getter
  @Setter
  @NotNull
  @Valid
  private MessageWrapper<OutputStream> output;


  @Override
  public void prepare() throws CoreException {
    Args.notNull(getInput(), "input");
    Args.notNull(getOutput(), "output");
    super.prepare();
  }

  
  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      AWSKMSClient client = awsClient();
      String key = msg.resolve(getKeyId());
      SigningAlgorithmSpec signingAlg = signingAlgorithm(msg);
      MessageType type = messageType(msg);
      byte[] dataToBeSigned = getInput().wrap(msg);
      ByteBuffer toBeSigned = ByteBuffer.wrap(dataToBeSigned);

      SignRequest request =  new SignRequest()
          .withKeyId(key)
          .withSigningAlgorithm(signingAlg)
          .withMessageType(type)
          .withMessage(toBeSigned);
      SignResult response = client.sign(request);
      ByteBuffer signedData = response.getSignature();
      if (extendedLogging()) {
        log.trace("Data:\n{}", HexDump.parse(dataToBeSigned));
        log.trace("Signature:\n{}", HexDump.parse(signedData.array()));
      }

      try (ByteBufferInputStream in = new ByteBufferInputStream(signedData);
          OutputStream out = getOutput().wrap(msg)) {
        IOUtils.copy(in, out);
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

  public GenerateSignatureService withInput(MessageWrapper<byte[]> w) {
    setInput(w);
    return this;
  }

  public GenerateSignatureService withOutput(MessageWrapper<OutputStream> w) {
    setOutput(w);
    return this;
  }

}
