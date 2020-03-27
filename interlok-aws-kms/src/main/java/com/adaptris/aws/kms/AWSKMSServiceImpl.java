package com.adaptris.aws.kms;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.ConnectedService;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.util.Args;
import com.amazonaws.services.kms.AWSKMSClient;
import lombok.Getter;
import lombok.Setter;

public abstract class AWSKMSServiceImpl extends ServiceImp implements ConnectedService {

  /**
   * The connection to AWS KMS.
   * 
   */
  @Valid
  @Getter
  @Setter
  @NotNull
  private AdaptrisConnection connection;

  /**
   * The ID of the key that you will be using.
   * 
   * <p>
   * Note that this is not the alias that you assigned when creating the key; it's the UUID associated with it
   * </p>
   */
  @Getter
  @Setter
  @InputFieldHint(expression = true)
  @NotBlank
  private String keyId;

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getKeyId(), "key-id");
    Args.notNull(getConnection(), "connection");
    LifecycleHelper.prepare(getConnection());
  }

  @Override
  protected void initService() throws CoreException {
    LifecycleHelper.init(getConnection());
  }

  @Override
  public void start() throws CoreException {
    super.start();
    LifecycleHelper.start(getConnection());
  }

  @Override
  public void stop() {
    super.stop();
    LifecycleHelper.stop(getConnection());
  }

  @Override
  protected void closeService() {
    LifecycleHelper.close(getConnection());
  }

  public <T extends AWSKMSServiceImpl> T withConnection(AdaptrisConnection c) {
    setConnection(c);
    return (T) this;
  }

  public <T extends AWSKMSServiceImpl> T withKeyId(String s) {
    setKeyId(s);
    return (T) this;
  }

  protected AWSKMSClient awsClient() throws Exception {
    return getConnection().retrieveConnection(AWSKMSConnection.class).awsClient();
  }
}
