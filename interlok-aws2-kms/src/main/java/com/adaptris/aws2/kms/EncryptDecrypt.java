package com.adaptris.aws2.kms;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.common.ByteArrayFromPayload;
import com.adaptris.core.common.PayloadOutputStreamWrapper;
import com.adaptris.interlok.types.MessageWrapper;
import com.adaptris.util.KeyValuePairBag;
import com.adaptris.util.KeyValuePairSet;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;

import javax.validation.Valid;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

@NoArgsConstructor
public abstract class EncryptDecrypt extends AWSKMSServiceImpl {

  /**
   * Specify the source for the encrypt/decrypt operation.
   * <p>
   * Since KMS encrypt/decrypt API uses {@link ByteBuffer} we use a {@link MessageWrapper} that wraps a byte array; this will mean
   * that the entire contents of the message payload will ultimately be resident in memory.
   * </p>
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "byte-array-from-payload")
  @Valid
  @AdvancedConfig
  private MessageWrapper<byte[]> input;

  /**
   * Specify the message type for this operation.
   * 
   * <p>
   * Since KMS encrypt/decrypt API uses {@link ByteBuffer} we use a {@link MessageWrapper} that wraps an Outputstream
   * </p>
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "payload-output-stream-wrapper")
  @Valid
  @AdvancedConfig
  private MessageWrapper<OutputStream> output;

  /**
   * Specify an encryption context values that you want to use.
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @Valid
  private KeyValuePairSet encryptionContext;

  /**
   * Specify the encryption algorithm to use for this operation.
   * <p>
   * No validation is done on the value; if this is not set then behaviour will largely be dependent on the key in question, if it
   * is a symmetric key then {@code SYMMETRIC_DEFAULT} will be used, if it is an asymmetric CMK then it's likely be an exception.
   * </p>
   */
  @Getter
  @Setter
  @InputFieldHint(expression = true, style = "com.amazonaws.services.kms.model.EncryptionAlgorithmSpec")
  private String encryptionAlgorithm;

  public <T extends EncryptDecrypt> T withInput(MessageWrapper<byte[]> in) {
    setInput(in);
    return (T) this;
  }

  public <T extends EncryptDecrypt> T withOutput(MessageWrapper<OutputStream> out) {
    setOutput(out);
    return (T) this;
  }

  public <T extends EncryptDecrypt> T withEncryptionContext(KeyValuePairSet ctx) {
    setEncryptionContext(ctx);
    return (T) this;
  }

  public <T extends EncryptDecrypt> T withEncryptionAlgorithm(String s) {
    setEncryptionAlgorithm(s);
    return (T) this;
  }

  protected Map<String, String> encryptionContext() {
    return KeyValuePairBag.asMap(ObjectUtils.defaultIfNull(getEncryptionContext(), new KeyValuePairSet()));
  }

  protected String encryptionAlgorithm(AdaptrisMessage msg) {
    if (getEncryptionAlgorithm() != null) {
      return msg.resolve(getEncryptionAlgorithm());
    }
    return null;
  }
  
  protected void writeOutput(ByteBuffer data, AdaptrisMessage msg) throws Exception {
    try (ByteBufferInputStream in = new ByteBufferInputStream(data);
        OutputStream out = output().wrap(msg)) {
      IOUtils.copy(in, out);
    }
  }

  protected ByteBuffer toByteBuffer(AdaptrisMessage msg) throws Exception {
    return ByteBuffer.wrap(input().wrap(msg));
  }

  private MessageWrapper<byte[]> input() {
    return ObjectUtils.defaultIfNull(getInput(), new ByteArrayFromPayload());
  }

  private MessageWrapper<OutputStream> output() {
    return ObjectUtils.defaultIfNull(getOutput(), new PayloadOutputStreamWrapper());
  }


}
