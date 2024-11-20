package com.adaptris.aws.kinesis;

import java.util.concurrent.atomic.AtomicInteger;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * Producer to amazon kinesis using the Kinesis Producer Library.
 *
 * <p>
 * Different from {@link KinesisStreamProducer} in that it waits for a confirmation of produce to the stream,
 * rather than offloading to the KPL.
 * </p>
 * <p>
 * This departs from a standard producer in the sense the {@link #getDestination()} can be regarded as optional. The reason
 * for this is that both {@code stream} and {@code partitionKey} are required elements, but {@link #getDestination()} only provides
 * a single method (of course we could change it to provide more). So the behaviour here is changed so that
 * <ul>
 * <li>if {@link #setStream(String)} is blank, then we use {@link #getDestination()}, otherwise
 * we use {@link #getStream()}.</li>
 * <li>{@link #setPartitionKey(String)} should always be populated with a non-blank value, which will be used.</li>
 * </ul>
 * </p>
 *
 * @config aws-kinesis-synchronous-stream-producer
 */
@ComponentProfile(summary = "Produce synchronously to Amazon Kinesis using the Kinesis Producer Library", tag = "amazon,aws,kinesis,producer",
    recommended = {ProducerLibraryConnection.class})
@XStreamAlias("aws-kinesis-synchronous-stream-producer")
@NoArgsConstructor
@DisplayOrder(order = {"stream", "partitionKey"})
public class KinesisSynchronousStreamProducer extends KinesisStreamProducer {

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    try {
      ListenableFuture<UserRecordResult> results = addUserRecord(msg, endpoint);
      UserRecordResult result = results.get();
      logUserRecordResult(result);
    } catch (Exception e) {
      if(e.getCause() instanceof UserRecordFailedException) {
        UserRecordFailedException userRecordFailedException = (UserRecordFailedException)e.getCause();
        logUserRecordResult(userRecordFailedException.getResult());
      }
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  private void logUserRecordResult(UserRecordResult result){
    log.debug("KPL Result: isSuccessful [{}], sequenceNumber [{}], shardId [{}], attempts [{}]",
      result.isSuccessful(),
      result.getSequenceNumber(),
      result.getShardId(),
      result.getAttempts().size()
    );
    AtomicInteger counter = new AtomicInteger(1);
    result.getAttempts().forEach((attempt) -> {
      int i =  counter.incrementAndGet();
      log.trace("Attempt [{}]: delay [{}], duration [{}], error code [{}], error message [{}]",
        i,
        attempt.getDelay(),
        attempt.getDuration(),
        attempt.getErrorCode(),
        attempt.getErrorMessage());
    });
  }
}
