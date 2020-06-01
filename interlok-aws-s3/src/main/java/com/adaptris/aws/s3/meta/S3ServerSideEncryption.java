/*
    Copyright 2018 Adaptris

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.aws.s3.meta;

import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.BooleanUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

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
  @InputFieldDefault(value = "true")
  @Getter
  @Setter
  private Boolean enabled;
  
  @NotNull
  @AutoPopulated
  @AdvancedConfig
  private Algorithm algorithm;
  
  public S3ServerSideEncryption() {
    setAlgorithm(Algorithm.AES_256);
  }

  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) {
    if (enabled()) {
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

  private boolean enabled() {
    return BooleanUtils.toBooleanDefaultIfNull(getEnabled(), true);
  }

}
