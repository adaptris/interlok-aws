package com.adaptris.aws.s3;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.aws.AWSAuthentication;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * Abstract implemention of {@link S3Service}
 * <p>
 * This class directly exposes almost all the getter and setters that are available in {@link ClientConfiguration} via the
 * {@link #getClientConfiguration()} property for maximum flexibility in configuration.
 * </p>
 * <p>
 * The key from the <code>client-configuration</code> element should match the name of the underlying ClientConfiguration
 * property; so if you wanted to control the user-agent you would do :
 * </p>
 * <pre>
 * {@code 
 *   <client-configuration>
 *     <key-value-pair>
 *        <key>UserAgent</key>
 *        <value>My User Agent</value>
 *     </key-value-pair>
 *   </client-configuration>
 * }
 * </pre>
 * 
 * @author lchan
 *
 */
public abstract class S3ServiceImpl extends ServiceImp {

  private transient AmazonS3Client s3;
  @Valid
  @NotNull
  @AutoPopulated
  private AWSAuthentication authentication;

  @Valid
  @NotNull
  @AutoPopulated
  private KeyValuePairSet clientConfiguration;

  public S3ServiceImpl() {
    setAuthentication(new DefaultAWSAuthentication());
    setClientConfiguration(new KeyValuePairSet());
  }

  @Override
  public void prepare() throws CoreException {}

  @Override
  protected void closeService() {}

  @Override
  protected void initService() throws CoreException {
    try {
      AWSCredentials creds = getAuthentication().getAWSCredentials();
      ClientConfiguration cc = ClientConfigurationBuilder.build(getClientConfiguration());
      AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().withClientConfiguration(cc);
      if (creds != null) {
        builder.withCredentials(new AWSStaticCredentialsProvider(creds));
      }
      s3 = (AmazonS3Client) builder.build();
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  protected AmazonS3Client amazonClient() {
    return s3;
  }

  public AWSAuthentication getAuthentication() {
    return authentication;
  }

  /**
   * The authentication method to use
   * 
   * @param authentication the authentication to use.
   */
  public void setAuthentication(AWSAuthentication authentication) {
    this.authentication = Args.notNull(authentication, "authentication");
  }


  /**
   * @return the configurationBuilder
   */
  public KeyValuePairSet getClientConfiguration() {
    return clientConfiguration;
  }


  /**
   * @param b the configurationBuilder to set
   */
  public void setClientConfiguration(KeyValuePairSet b) {
    this.clientConfiguration = Args.notNull(b, "configurationBuilder");
  }
}
