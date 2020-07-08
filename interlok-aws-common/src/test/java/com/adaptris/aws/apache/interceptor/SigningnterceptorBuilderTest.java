package com.adaptris.aws.apache.interceptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.apache.http.HttpRequestInterceptor;
import org.junit.Test;
import com.adaptris.aws.AWSCredentialsProviderBuilder;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.aws.apache.interceptor.ApacheSigningInterceptor;
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

  private class FailingCredentialsBuilder implements AWSCredentialsProviderBuilder {

    @Override
    public AWSCredentialsProvider build() throws Exception {
      throw new Exception();
    }
    
  }
}
