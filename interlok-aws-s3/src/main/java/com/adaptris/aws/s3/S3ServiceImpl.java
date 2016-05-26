package com.adaptris.aws.s3;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.aws.AWSAuthentication;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;

public abstract class S3ServiceImpl extends ServiceImp {

  private transient AmazonS3Client s3;
  @Valid
  @NotNull
  @AutoPopulated
  private AWSAuthentication authentication;

  public S3ServiceImpl() {
    setAuthentication(new DefaultAWSAuthentication());
  }

  @Override
  public void prepare() throws CoreException {}

  @Override
  protected void closeService() {}

  @Override
  protected void initService() throws CoreException {
    try {
      AWSCredentials creds = getAuthentication().getAWSCredentials();
      if (creds != null) {
        s3 = new AmazonS3Client(new StaticCredentialsProvider(creds));
      } else {
        s3 = new AmazonS3Client();
      }
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
   */
  public void setAuthentication(AWSAuthentication authentication) {
    this.authentication = Args.notNull(authentication, "authentication");
  }

}
