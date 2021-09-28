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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.ConnectedService;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.util.Args;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Abstract implemention of {@link S3Service}
 * 
 */
public abstract class S3ServiceImpl extends ServiceImp implements ConnectedService {

  /**
   * Set the connection to use to connect to S3.
   * 
   */
  @Valid
  @Getter
  @Setter
  @NotNull
  @NonNull
  private AdaptrisConnection connection;
  
  public S3ServiceImpl() {
  }

  @Override
  public void prepare() throws CoreException {
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
}
