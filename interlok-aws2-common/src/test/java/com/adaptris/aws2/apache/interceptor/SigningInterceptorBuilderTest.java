package com.adaptris.aws2.apache.interceptor;

import com.adaptris.aws2.AWSCredentialsProviderBuilder;
import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.apache.http.HttpRequestInterceptor;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

  @Test(expected=Exception.class)
  public void testBuild_Exception() {
    ApacheSigningInterceptor builder =
        new ApacheSigningInterceptor().withRegion("region").withService("service")
            .withCredentials(new FailingCredentialsBuilder());
    HttpRequestInterceptor interceptor = builder.build();
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
