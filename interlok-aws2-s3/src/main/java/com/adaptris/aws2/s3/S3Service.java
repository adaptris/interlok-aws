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

package com.adaptris.aws2.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 *
 * @config amazon-s3-service
 * @since 4.3.0
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Service", recommended={AmazonS3Connection.class}, since = "4.3.0")
@XStreamAlias("aws2-amazon-s3-service")
@DisplayOrder(order = {"connection", "operation"})
@NoArgsConstructor
public class S3Service extends S3ServiceImpl {

  @NotNull
  @Valid
  private S3Operation operation;

  public S3Service(AdaptrisConnection c, S3Operation op) {
    this();
    setConnection(c);
    setOperation(op);
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      getOperation().execute(getConnection().retrieveConnection(ClientWrapper.class), msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

  @Override
  public void prepare() throws CoreException {
    Args.notNull(getOperation(), "operation");
    super.prepare();
    getOperation().prepare();
  }


  public S3Operation getOperation() {
    return operation;
  }

  public void setOperation(S3Operation operation) {
    this.operation = Args.notNull(operation, "operation");
  }

}
