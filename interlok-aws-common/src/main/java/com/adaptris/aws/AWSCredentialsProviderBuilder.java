package com.adaptris.aws;

import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.Removal;
import com.adaptris.core.util.LoggingHelper;
import com.amazonaws.auth.AWSCredentialsProvider;

@FunctionalInterface
public interface AWSCredentialsProviderBuilder {

  AWSCredentialsProvider build() throws Exception;

  /**
   * Helper to log warnings when configuration contains an {@link AWSAuthentication} member rather
   * than a {@link AWSCredentialsProviderBuilder}.
   * 
   * @deprecated will be removed as soon as {@link AWSAuthentication} is removed from various
   *             connections.
   */
  @Deprecated
  @Removal(message = "will be removed in a future release w/o warning")
  static AWSCredentialsProviderBuilder providerWithWarning(String source, AWSAuthentication auth,
      AWSCredentialsProviderBuilder builder) {
    if (auth != null) {
      LoggingHelper.logWarning(false, () -> {
      }, "authentication is deprecated in {}; use a AWSCredentialsProviderBuilder instead", source);
      return new StaticCredentialsBuilder().withAuthentication(auth);
    }
    return ObjectUtils.defaultIfNull(builder, new StaticCredentialsBuilder().withAuthentication(new DefaultAWSAuthentication()));
  }

}
