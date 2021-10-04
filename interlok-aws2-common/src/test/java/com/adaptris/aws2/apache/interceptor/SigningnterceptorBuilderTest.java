package com.adaptris.aws2.apache.interceptor;

import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.apache.http.HttpRequestInterceptor;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SigningnterceptorBuilderTest {

  @Test
  public void testBuild() {
    ApacheSigningInterceptor builder =
        new ApacheSigningInterceptor().withRegion("region").withService("service")
            .withCredentials(StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret")));
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

  private class FailingCredentialsBuilder implements AwsCredentialsProvider {
    @Override
    public AwsCredentials resolveCredentials()
    {
      throw new RuntimeException();
    }
  }
}
