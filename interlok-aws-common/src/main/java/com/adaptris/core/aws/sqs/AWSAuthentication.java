package com.adaptris.core.aws.sqs;

import com.adaptris.security.exc.AdaptrisSecurityException;
import com.amazonaws.auth.AWSCredentials;

public interface AWSAuthentication {
    
  public AWSCredentials getAWSCredentials() throws AdaptrisSecurityException;
  
}
