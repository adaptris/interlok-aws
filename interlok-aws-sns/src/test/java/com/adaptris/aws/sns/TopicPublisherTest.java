package com.adaptris.aws.sns;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.sns.AmazonSNSConnection;
import com.adaptris.aws.sns.PublishToTopic;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;

public class TopicPublisherTest extends ProducerCase {

  
  public TopicPublisherTest(String params) {
    super(params);
  }
  
  @Override
  protected StandaloneProducer retrieveObjectForSampleConfig() {
    PublishToTopic producer = new PublishToTopic();
    producer.setDestination(new ConfiguredProduceDestination("arn:aws:sns:us-east-1:123456789012:MyNewTopic"));
    
    AmazonSNSConnection conn = new AmazonSNSConnection();
    AWSKeysAuthentication kauth = new AWSKeysAuthentication();
    kauth.setAccessKey("accessKey");
    kauth.setSecretKey("secretKey");
    conn.setAuthentication(kauth);
    conn.setRegion("My AWS Region");
    StandaloneProducer result = new StandaloneProducer(conn, producer);
    return result;
  }


}
