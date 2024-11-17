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

package com.adaptris.aws.kinesis;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws.AWSConnection;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon Kinesis using the SDK.
 *
 * <p>This may be the preferred approach over using KPL if you're running in environment where you don't want other
 * processes to be spawned (for example: containerised).</p>
 *
 * @config aws-kinesis-sdk-connection
 */
@XStreamAlias("aws-kinesis-sdk-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting connectivity to Amazon Kinesis", tag = "connections,amazon,aws,kinesis",
    since = "3.12.1")
@DisplayOrder(order = {"region", "authentication", "clientConfiguration", "retryPolicy",
    "customEndpoint"})
@NoArgsConstructor
public class AWSKinesisSDKConnection extends AWSConnection {

  private transient AmazonKinesis kinesis;

  @Override
  protected void prepareConnection() throws CoreException {
    //not needed for connection
  }

  @Override
  protected void initConnection() throws CoreException {
    AmazonKinesisClientBuilder builder = createBuilder();
    kinesis = builder.build();
  }

  @Override
  protected void startConnection() throws CoreException {
    //not needed for connection
  }

  @Override
  protected void stopConnection() {
    // not needed for connection
  }


  @Override
  protected void closeConnection() {
    if (kinesis != null) {
      kinesis.shutdown();
      kinesis = null;
    }
  }

  protected AmazonKinesisClientBuilder createBuilder() throws CoreException {
    AmazonKinesisClientBuilder builder;
    try {
      ClientConfiguration cc =
          ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());
      builder = endpointBuilder().rebuild(AmazonKinesisClientBuilder.standard().withClientConfiguration(cc));
      builder.withCredentials(credentialsProvider().build(this));
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
    return builder;
  }

  public AmazonKinesis kinesisClient() {
    return kinesis;
  }

}
