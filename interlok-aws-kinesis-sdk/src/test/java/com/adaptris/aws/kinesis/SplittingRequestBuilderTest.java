package com.adaptris.aws.kinesis;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.services.splitter.NoOpSplitter;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import org.junit.Test;

import static org.junit.Assert.*;

public class SplittingRequestBuilderTest {

  @Test
  public void messageSplitter(){
    SplittingRequestBuilder builder = new SplittingRequestBuilder();
    try {
      builder.setMessageSplitter(null);
      fail();
    } catch (NullPointerException e){
      assertEquals("messageSplitter is marked non-null but is null", e.getMessage());
    }
    builder.setMessageSplitter(new NoOpSplitter());
    assertTrue(builder.getMessageSplitter() instanceof NoOpSplitter);
  }

  @Test
  public void iterator() throws Exception {
    SplittingRequestBuilder builder = new SplittingRequestBuilder()
      .withMessageSplitter(new NoOpSplitter());
    AdaptrisMessage message = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    Iterable<PutRecordsRequestEntry> iterable = builder.build("key", message);
    iterable.iterator();
    try {
      iterable.iterator();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("iterator already invoked", e.getMessage());
    }
  }

}
