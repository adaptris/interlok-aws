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

import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon Kinesis using the Kinesis Producer Library.
 * 
 * 
 */
public abstract class ProducerLibraryConnection extends AdaptrisConnectionImp implements KinesisProducerWrapper {

  protected transient KinesisProducer producer;

  public ProducerLibraryConnection() {}

  @Override
  protected void prepareConnection() throws CoreException {}

  @Override
  protected void initConnection() throws CoreException {}

  @Override
  protected void startConnection() throws CoreException {}

  @Override
  protected void stopConnection() {}

  @Override
  protected void closeConnection() {
    shutdownQuietly(producer);
    producer = null;
  }

  protected static void shutdownQuietly(KinesisProducer p) {
    if (p != null) {
      p.flushSync();
      p.destroy();
    }
  }
}
