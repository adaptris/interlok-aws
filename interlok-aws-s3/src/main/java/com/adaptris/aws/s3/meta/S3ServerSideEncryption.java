package com.adaptris.aws.s3.meta;

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Enable S3 Server Side Encryption with AWS managed keys
 */
@XStreamAlias("s3-serverside-encryption")
public class S3ServerSideEncryption extends S3ObjectMetadata {

  public enum Algorithm {
    AES_256(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

    private final String name;
    
    private Algorithm(String name) {
      this.name = name;
    }
    
    public void apply(ObjectMetadata meta) {
      meta.setSSEAlgorithm(name);
    }
  }

  @AdvancedConfig
  @InputFieldDefault("true")
  private boolean enabled = true;
  
  @NotNull
  @AutoPopulated
  private Algorithm algorithm;
  
  public S3ServerSideEncryption() {
    setEnabled(true);
    setAlgorithm(Algorithm.AES_256);
  }

  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) {
    if(getEnabled()) {
      getAlgorithm().apply(meta);
    }
  }

  public Algorithm getAlgorithm() {
    return algorithm;
  }

  /**
   * Set the algorithm for server side encryption
   * 
   * @param algorithm
   */
  public void setAlgorithm(Algorithm algorithm) {
    this.algorithm = algorithm;
  }

  public boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

}
