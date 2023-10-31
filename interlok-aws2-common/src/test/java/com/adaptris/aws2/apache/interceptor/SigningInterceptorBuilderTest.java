package com.adaptris.aws2.apache.interceptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.http.HttpRequestInterceptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.adaptris.aws2.AWSCredentialsProviderBuilder;
import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class SigningInterceptorBuilderTest {

  @Test
  public void testBuild() {
    ApacheSigningInterceptor builder =
        new ApacheSigningInterceptor().withRegion("eu-west-1").withService("service")
            .withCredentials(new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("access", "secret")));
    HttpRequestInterceptor interceptor = builder.build();
    assertNotNull(interceptor);
    assertEquals(AWSRequestSigningApacheInterceptor.class, interceptor.getClass());

  }

  @Test
  public void testBuild_Exception() {
    Assertions.assertThrows(Exception.class, () -> {
      ApacheSigningInterceptor builder =
          new ApacheSigningInterceptor().withRegion("region").withService("service")
              .withCredentials(new FailingCredentialsBuilder());
      builder.build();
    });
  }

  private class FailingCredentialsBuilder implements AWSCredentialsProviderBuilder
  {
    @Override
    public AwsCredentialsProvider build() throws Exception
    {
      throw new RuntimeException();
    }
  }
}
