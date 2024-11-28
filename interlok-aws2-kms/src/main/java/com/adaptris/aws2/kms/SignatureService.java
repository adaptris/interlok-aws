package com.adaptris.aws2.kms;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.kms.model.MessageType;
import software.amazon.awssdk.services.kms.model.SigningAlgorithmSpec;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

@NoArgsConstructor
public abstract class SignatureService extends AWSKMSServiceImpl {

  /**
   * Specify the signing algorithm to use for this operation.
   * <p>
   * No validation is done on the value, other than to check that it's a support algorithm via
   * {@link SigningAlgorithmSpec#fromValue(String)}. Choose an algorithm that is compatible with the type and size of the specified
   * asymmetric CMK. The default is {@code RSASSA_PKCS1_V1_5_SHA_256} if not specified.
   * </p>
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "RSASSA_PKCS1_V1_5_SHA_256")
  @InputFieldHint(expression = true, style = "com.amazonaws.services.kms.model.SigningAlgorithmSpec")
  @NotBlank
  private String signingAlgorithm;

  /**
   * Specify the message type for this operation.
   * 
   * <p>
   * No validation is done on the value other than to check that it's supported via {@code MessageType#fromValue(String)}. The
   * default is {@code RAW} if not specified.
   * </p>
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "RAW")
  @InputFieldHint(expression = true, style = "com.amazonaws.services.kms.model.MessageType")
  @Valid
  private String messageType;

  @Getter
  @Setter
  @AdvancedConfig(rare = true)
  @InputFieldDefault(value = "false")
  private Boolean extendedLogging;

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
    return StringUtils.defaultIfBlank(getSigningAlgorithm(), SigningAlgorithmSpec.RSASSA_PKCS1_V1_5_SHA_256.name());
  }


  public MessageType messageType(AdaptrisMessage msg) {
    return MessageType.fromValue(msg.resolve(messageType()));
  }

  private String messageType() {
    return StringUtils.defaultIfBlank(getMessageType(), MessageType.RAW.name());
  }

  protected boolean extendedLogging() {
    return BooleanUtils.toBooleanDefaultIfNull(getExtendedLogging(), false);
  }
}
