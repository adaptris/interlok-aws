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

package com.adaptris.aws2.s3.meta;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * Enable S3 Server Side Encryption with AWS managed keys
 */
@XStreamAlias("aws2-s3-serverside-encryption")
public class S3ServerSideEncryption extends S3ObjectMetadata {

  private static final String SERVER_SIDE_ENCRYPTION = "x-amz-server-side-encryption"; // From Headers.SERVER_SIDE_ENCRYPTION

  public enum Algorithm {
    AES_256(ServerSideEncryption.AES256);

    private final ServerSideEncryption name;

    Algorithm(ServerSideEncryption name) {
      this.name = name;
    }

    public void apply(Map<String, String> meta) {
      meta.put(SERVER_SIDE_ENCRYPTION, name.toString());
    }
  }

  /**
   * Whether or not to actually enable server side encryption.
   *
   * <p>
   * This is arguably meaningless since why would you want to define s3-serverside-encryption if you
   * didn't want it, however, it can be useful as a variable substitution flag where some
   * environments might not need it.
   * </p>
   */
  @AdvancedConfig(rare = true)
  @InputFieldDefault(value = "true")
  @Getter
  @Setter
  private Boolean enabled;

  /**
   * Set the algorithm for server side encryption
   *
   */
  @NotNull
  @AutoPopulated
  @AdvancedConfig
  @Getter
  @Setter
  @NonNull
  private Algorithm algorithm;

  public S3ServerSideEncryption() {
    setAlgorithm(Algorithm.AES_256);
  }

  @Override
  public void apply(AdaptrisMessage msg, Map<String, String> meta) {
    if (enabled()) {
      getAlgorithm().apply(meta);
    }
  }

  private boolean enabled() {
    return BooleanUtils.toBooleanDefaultIfNull(getEnabled(), true);
  }

}
