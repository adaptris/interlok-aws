package com.adaptris.aws;

import com.adaptris.security.exc.AdaptrisSecurityException;
import com.amazonaws.auth.AWSCredentials;

public interface AWSAuthentication {
    
  public AWSCredentials getAWSCredentials() throws AdaptrisSecurityException;
  
}
