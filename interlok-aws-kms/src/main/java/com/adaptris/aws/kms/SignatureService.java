package com.adaptris.aws.kms;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.StringUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.amazonaws.services.kms.model.MessageType;
import com.amazonaws.services.kms.model.SigningAlgorithmSpec;
import lombok.Getter;
import lombok.Setter;

public abstract class SignatureService extends AWSKMSServiceImpl {

  /**
   * Specify the signing algorithm to use for this operation.
   * <p>
   * No validation is done on the value, other than to check that it's a support algorithm via
   * {@link SigningAlgorithmSpec#fromValue(String)}. Choose an algorithm that is compatible with the type and size of the specified
   * asymmetric CMK.
   * </p>
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "RSASSA_PSS_SHA_256")
  @InputFieldHint(expression = true, style = "com.amazonaws.services.kms.model.SigningAlgorithmSpec")
  @NotBlank
  private String signingAlgorithm;

  /**
   * Specify the message type for this operation.
   * 
   * <p>
   * Use {@code RAW} if the message to be signed is less than 4096 bytes, or use {@code DIGEST} and make sure it is a digest using
   * (@link PayloadHashingService} or similar. The default is {@code RAW} if not specified.
   * </p>
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "RAW")
  @InputFieldHint(expression = true, style = "com.amazonaws.services.kms.model.MessageType")
  @Valid
  @AdvancedConfig
  private String messageType;

  public <T extends SignatureService> T withMessageType(String s) {
    setMessageType(s);
    return (T) this;
  }

  public <T extends SignatureService> T withSigningAlgorithm(String s) {
    setSigningAlgorithm(s);
    return (T) this;
  }


  public SigningAlgorithmSpec signingAlgorithm(AdaptrisMessage msg) {
    return SigningAlgorithmSpec.fromValue(msg.resolve(signingAlgorithm()));
  }

  private String signingAlgorithm() {
    return StringUtils.defaultIfBlank(getSigningAlgorithm(), SigningAlgorithmSpec.RSASSA_PSS_SHA_256.name());
  }


  public MessageType messageType(AdaptrisMessage msg) {
    return MessageType.fromValue(msg.resolve(messageType()));
  }

  private String messageType() {
    return StringUtils.defaultIfBlank(getMessageType(), MessageType.RAW.name());
  }

}
