package com.adaptris.aws2.apache.interceptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.apache.http.HttpRequestInterceptor;
import org.junit.Test;
import com.adaptris.aws2.AWSCredentialsProviderBuilder;
import com.adaptris.aws2.STSAssumeroleCredentialsBuilder;
import com.adaptris.aws2.StaticCredentialsBuilder;
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

  @Test(expected=Exception.class)
  public void testBuild_Exception() {
    ApacheSigningInterceptor builder =
        new ApacheSigningInterceptor().withRegion("region").withService("service")
            .withCredentials(new FailingCredentialsBuilder());
    HttpRequestInterceptor interceptor = builder.build();
  }

  @Test
  public void testBuild_WithSTS() {
    STSAssumeroleCredentialsBuilder sts =
        new STSAssumeroleCredentialsBuilder().withRoleArn("arn:aws2:sts:us-west-1:123456789012:MyArn")
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
