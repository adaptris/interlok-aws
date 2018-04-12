package com.adaptris.aws.s3;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * 
 * @author gdries
 * @config amazon-s3-service
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Service", recommended={AmazonS3Connection.class})
@XStreamAlias("amazon-s3-service")
@DisplayOrder(order = {"connection", "operation"})
public class S3Service extends S3ServiceImpl {

  @NotNull
  @Valid
  private S3Operation operation;

  public S3Service() {}

  public S3Service(AdaptrisConnection c, S3Operation op) {
    this();
    setConnection(c);
    setOperation(op);
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      getOperation().execute(getConnection().retrieveConnection(AmazonS3Connection.class), msg);
    } catch (InterlokException e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

  @Override
  protected void initService() throws CoreException {
    super.initService();
    if (getOperation() == null) {
      throw new CoreException("No operation configured");
    }
  }

  public S3Operation getOperation() {
    return operation;
  }

  public void setOperation(S3Operation operation) {
    this.operation = Args.notNull(operation, "operation");
  }

}
