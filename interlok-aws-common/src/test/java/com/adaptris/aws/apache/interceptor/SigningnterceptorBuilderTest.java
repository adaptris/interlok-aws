package com.adaptris.aws.apache.interceptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.http.HttpRequestInterceptor;
import org.junit.jupiter.api.Test;
import com.adaptris.aws.AWSCredentialsProviderBuilder;
import com.adaptris.aws.STSAssumeroleCredentialsBuilder;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;

public class SigningnterceptorBuilderTest {

  @Test
  public void testBuild() {
    ApacheSigningInterceptor builder =
        new ApacheSigningInterceptor().withRegion("region").withService("service")
            .withCredentials(new StaticCredentialsBuilder());
    HttpRequestInterceptor interceptor = builder.build();
    assertNotNull(interceptor);
    assertEquals(AWSRequestSigningApacheInterceptor.class, interceptor.getClass());

  }

  @Test
  public void testBuild_Exception() {
    ApacheSigningInterceptor builder =
        new ApacheSigningInterceptor().withRegion("region").withService("service")
            .withCredentials(new FailingCredentialsBuilder());
    assertThrows(Exception.class, ()->{
      HttpRequestInterceptor interceptor = builder.build();
    }, "Failed to build");
  }

  @Test
  public void testBuild_WithSTS() {
    STSAssumeroleCredentialsBuilder sts =
        new STSAssumeroleCredentialsBuilder().withRoleArn("arn:aws:sts:us-west-1:123456789012:MyArn")
            .withRoleExternalId("externalId").withRoleSessionName("sessionName");
    ApacheSigningInterceptor builder = new ApacheSigningInterceptor().withRegion("region")
        .withService("service").withCredentials(sts);
    HttpRequestInterceptor interceptor = builder.build();
    assertNotNull(interceptor);
    assertEquals(AWSRequestSigningApacheInterceptor.class, interceptor.getClass());

  }

  private class FailingCredentialsBuilder implements AWSCredentialsProviderBuilder {

    @Override
    public AWSCredentialsProvider build() throws Exception {
      throw new Exception();
    }

  }
}
