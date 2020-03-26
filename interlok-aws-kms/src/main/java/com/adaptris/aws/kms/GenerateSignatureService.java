package com.adaptris.aws.kms;

import java.io.InputStream;
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
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.types.MessageWrapper;
import com.adaptris.interlok.util.Args;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SignRequest;
import com.amazonaws.services.kms.model.SignResult;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

/**
 * 
 * @config aws-kms-signing
 */
@AdapterComponent
@ComponentProfile(summary = "Signing via AWS KMS", recommended = {AWSKMSConnection.class})
@XStreamAlias("aws-kms-generate-signature")
@DisplayOrder(order = {"connection", "id", "signatureAlgorithm"})
public class GenerateSignatureService extends SignatureService {

  @Getter
  @Setter
  @NotNull
  @Valid
  private MessageWrapper<byte[]> input;
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
      ByteBuffer toBeSigned = ByteBuffer.wrap(input.wrap(msg));
      SignRequest request =  new SignRequest()
          .withKeyId(key)
          .withSigningAlgorithm(signingAlg)
          .withMessageType(type)
          .withMessage(toBeSigned);
      SignResult response = client.sign(request);
      ByteBuffer signedData = response.getSignature();
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


  public static class ByteBufferInputStream extends InputStream {
    private final ByteBuffer wrappedBuffer;

    public ByteBufferInputStream(ByteBuffer buf) {
      wrappedBuffer = buf;
    }

    @Override
    public int read() {
      if (!wrappedBuffer.hasRemaining()) {
        return -1;
      }
      return wrappedBuffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int off, int len) {
      if (!wrappedBuffer.hasRemaining()) {
        return -1;
      }
      len = Math.min(len, wrappedBuffer.remaining());
      wrappedBuffer.get(bytes, off, len);
      return len;
    }
  }
}
