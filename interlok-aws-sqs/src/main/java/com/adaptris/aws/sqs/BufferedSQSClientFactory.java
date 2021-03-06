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

package com.adaptris.aws.sqs;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.aws.EndpointBuilder;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.buffered.QueueBufferConfig;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

/**
 * Buffered SQS Client Factory.
 * 
 * @config buffered-sqs-client-factory
 * @since 3.0.3
 */
@XStreamAlias("buffered-sqs-client-factory")
public class BufferedSQSClientFactory extends UnbufferedSQSClientFactory {

  /**
   * Specifies the maximum number of entries the bufferinc client will put in a single batch request.
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private int maxBatchSize;
  
  /**
   * Specifies the amount of time, in seconds, the receive call will block on the server waiting for
   * messages to arrive if the queue is empty when the receive call is first made. This setting has no
   * effect if long polling is disabled.
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private int longPollWaitTimeoutSeconds;
  
  /**
   * Custom visibility timeout to use when retrieving messages from SQS. If set to a value greater
   * than zero, this timeout will override the default visibility timeout set on the SQS queue. Set it
   * to -1 to use the default visiblity timeout of the queue. Visibility timeout of 0 seconds is not
   * supported.
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private int visibilityTimeoutSeconds;
  
  /**
   * Maximum permitted size of a SendMessage or SendMessageBatch message, in bytes. This setting is
   * also enforced on the server, and if this client submits a request of a size larger than the
   * server can support, the server will reject the request.
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private long maxBatchSizeBytes;
  
  /**
   * If more than that number of completed receive batches are waiting in the buffer, the querying for
   * new messages will stop. The larger this number, the more messages the queue buffer will pre-fetch
   * and keep in the buffer on the client side, and the faster receive requests will be satisfied. The
   * visibility timeout of a pre-fetched message starts at the point of pre-fetch, which means that
   * while the message is in the local buffer it is unavailable for other clients to process, and when
   * this client retrieves it, part of the visibility timeout may have already expired. The number of
   * messages prefetched will not exceed 10 * maxDoneReceiveBatches, as there can be a maximum of 10
   * messages per batch.
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private int maxDoneReceiveBatches;
  
  /**
   * The maximum number of concurrent receive message batches. The greater this number, the faster the
   * queue will be pulling messages from the SQS servers (at the expense of consuming more threads).
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private int maxInflightReceiveBatches;
  
  /**
   * The maximum number of concurrent batches for each type of outbound request. The greater the
   * number, the greater the throughput that can be achieved (at the expense of consuming more
   * threads).
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private int maxInflightOutboundBatches;
  
  /**
   * Specify "true" for receive requests to use long polling.
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private boolean longPoll;

  /**
   * The maximum time (milliseconds) a send batch is held open for additional outbound requests. The
   * longer this timeout, the longer messages wait for other messages to be added to the batch.
   * Increasing this timeout reduces the number of calls made and increases throughput, but also
   * increases average message latency.
   * 
   */
  @Getter
  @Setter
  @AdvancedConfig
  @AutoPopulated
  private long maxBatchOpenMs;

  public BufferedSQSClientFactory() {
    QueueBufferConfig config = new QueueBufferConfig();
    setLongPoll(config.isLongPoll());
    setLongPollWaitTimeoutSeconds(config.getLongPollWaitTimeoutSeconds());
    setMaxBatchOpenMs(config.getMaxBatchOpenMs());
    setMaxBatchSize(config.getMaxBatchSize());
    setMaxBatchSizeBytes(config.getMaxBatchSizeBytes());
    setMaxDoneReceiveBatches(config.getMaxDoneReceiveBatches());
    setMaxInflightOutboundBatches(config.getMaxInflightOutboundBatches());
    setMaxInflightReceiveBatches(config.getMaxInflightReceiveBatches());
    setVisibilityTimeoutSeconds(config.getVisibilityTimeoutSeconds());
  }
  
  @Override
  public AmazonSQSAsync createClient(AWSCredentialsProvider creds, ClientConfiguration conf, EndpointBuilder endpoint) {
    QueueBufferConfig config = new QueueBufferConfig()
      .withLongPoll(getLongPoll())
      .withLongPollWaitTimeoutSeconds(getLongPollWaitTimeoutSeconds())
      .withMaxBatchOpenMs(getMaxBatchOpenMs())
      .withMaxBatchSize(getMaxBatchSize())
      .withMaxBatchSizeBytes(getMaxBatchSizeBytes())
      .withMaxDoneReceiveBatches(getMaxDoneReceiveBatches())
      .withMaxInflightOutboundBatches(getMaxInflightOutboundBatches())
      .withMaxInflightReceiveBatches(getMaxInflightReceiveBatches())
      .withVisibilityTimeoutSeconds(getVisibilityTimeoutSeconds());
    AmazonSQSAsync realClient = super.createClient(creds, conf, endpoint);
    return new AmazonSQSBufferedAsyncClient(realClient, config);
  }
}
