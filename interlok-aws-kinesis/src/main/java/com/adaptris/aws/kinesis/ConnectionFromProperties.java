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

import java.io.InputStream;
import java.util.Properties;

import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotBlank;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.PropertyHelper;
import com.adaptris.util.URLHelper;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon Kinesis using the Kinesis Producer Library.
 * 
 * <p>
 * This derives its connection information from a properties file such as this <a href=
 * "https://raw.githubusercontent.com/awslabs/amazon-kinesis-producer/master/java/amazon-kinesis-producer-sample/default_config.properties">github
 * sample</a>. Configure the location as the {@link #setConfigLocation(String)} and it will be used to build the producer. This
 * effectively means that you will always be using a {@code DefaultAWSCredentialsProviderChain} since it is impossible to
 * override the {@code AWSCredentialsProvider} via the property file.
 * </p>
 * 
 * @config aws-kinesis-kpl-connection-from-properties
 */
@XStreamAlias("aws-kinesis-kpl-connection-from-properties")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting connectivity to AWS Kinesis", tag = "connections,amazon,aws,kinesis")
@DisplayOrder(order = {"configLocation"})
@NoArgsConstructor
public class ConnectionFromProperties extends ProducerLibraryConnection implements KinesisProducerWrapper {

  /**
   * The location of the property file that contains the kinesis producer library configuration.
   * <p>
   * This should be a URL style path (e.g. file:///path/to/my/properties).
   * </p>
   * 
   */
  @NotBlank
  @Getter
  @Setter
  @NonNull
  private String configLocation;

  @Override
  protected void initConnection() throws CoreException {
    Args.notBlank(getConfigLocation(), "config-location");
    super.initConnection();
  }

  public ConnectionFromProperties withConfigLocation(String s) {
    setConfigLocation(s);
    return this;
  }

  protected static Properties readConfig(String loc) throws Exception {
    try (InputStream in = URLHelper.connect(loc)) {
      return PropertyHelper.load(() -> {
        return in;
      });
    }
  }

  @Override
  public synchronized KinesisProducer kinesisProducer() throws Exception {
    if (producer == null) {
      producer = new KinesisProducer(KinesisProducerConfiguration.fromProperties(readConfig(getConfigLocation())));
    }
    return producer;
  }
}
