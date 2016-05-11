package com.adaptris.core.aws.sqs.jms;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.BooleanUtils;
import org.apache.http.util.Args;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazon.sqs.javamessaging.SQSConnectionFactory.Builder;
import com.amazonaws.ClientConfiguration;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * JMS VendorImplementation for Amazon SQS.
 * <p>
 * This VendorImplementation uses the Amazon SQS JMS compatibility layer. When using this class, do not use the AmazonSQS Producer
 * and Consumer classes. Use regular JMS consumers and producers instead.
 * </p>
 * <p>
 * This vendor implementation class directly exposes almost all the getter and setters that are available in the
 * {@link SQSConnectionFactory.Builder#getClientConfiguration()}
 * for maximum flexibility in configuration.
 * </p>
 * <p>
 * The key from the <code>connection-factory-properties</code> element should match the name of the underlying ClientConfiguration
 * property.
 * </p>
 * <pre>
 * {@code 
 *   <client-configuration-properties>
 *     <key-value-pair>
 *        <key>UserAgent</key>
 *        <value>My User Agent</value>
 *     </key-value-pair>
 *   </client-configuration-properties>
 * }
 * </pre>
 * will invoke {@link ClientConfiguration#withUserAgent(String)}, setting the UserAgent property.
 * 
 * @config advanced-amazon-sqs-implementation
 * @license STANDARD
 * @since 3.2.1
 */
@XStreamAlias("advanced-amazon-sqs-implementation")
public class AdvancedSQSImplementation extends AmazonSQSImplementation {

  public enum ClientConfigurationProperties {
    /**
     * Invokes {@link ClientConfiguration#withConnectionTimeout(int)}; the value is in MILLISECONDS
     * 
     */
    ConnectionTimeout() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withConnectionTimeout(Integer.parseInt(str));
      }      
    },
    /**
     * Invokes {@link ClientConfiguration#withConnectionTTL(long)}; the value is in MILLISECONDS
     * 
     */
    ConnectionTTL() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withConnectionTTL(Long.parseLong(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withGzip(boolean)}
     */
    Gzip() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withGzip(BooleanUtils.toBoolean(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withLocalAddress(InetAddress)} using {@link InetAddress#getByName(String)}
     * 
     */
    LocalAddress() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) throws UnknownHostException {
        return cc.withLocalAddress(InetAddress.getByName(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withMaxConnections(int)}
     * 
     */
    MaxConnections() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withMaxConnections(Integer.parseInt(str));
      }
    },

    /**
     * Invokes {@link ClientConfiguration#withMaxErrorRetry(int)}
     * 
     */
    MaxErrorRetry() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withMaxErrorRetry(Integer.parseInt(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withPreemptiveBasicProxyAuth(boolean)}
     * 
     */
    PreemptiveBasicProxyAuth() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withPreemptiveBasicProxyAuth(BooleanUtils.toBoolean(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withProtocol(com.amazonaws.Protocol)}
     * 
     */
    Protocol() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withProtocol(com.amazonaws.Protocol.valueOf(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withProxyDomain(String)}
     * 
     */
    ProxyDomain() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withProxyDomain(str);
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withProxyHost(String)}
     * 
     */
    ProxyHost() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withProxyHost(str);
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withProxyPassword(String)}
     * 
     */
    ProxyPassword() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withProxyPassword(str);
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withProxyPort(int)}
     * 
     */
    ProxyPort() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withProxyPort(Integer.parseInt(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withProxyUsername(String)}
     * 
     */
    ProxyUsername() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withProxyUsername(str);
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withProxyWorkstation(String)}
     * 
     */
    ProxyWorkstation() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withProxyWorkstation(str);
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withResponseMetadataCacheSize(int)}
     * 
     */
    ResponseMetadataCacheSize() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withResponseMetadataCacheSize(Integer.parseInt(str));
      }
    },

    /**
     * Invokes {@link ClientConfiguration#withReaper(boolean)}
     * 
     */
    Reaper() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withReaper(BooleanUtils.toBoolean(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withSignerOverride(String)}
     * 
     */
    SignerOverride() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withSignerOverride(str);
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withSocketBufferSizeHints(int, int)} after parsing a comma separated string.
     * 
     */
    SocketBufferSizeHints() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        String[] split = str.split(",");
        return cc.withSocketBufferSizeHints(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withSocketTimeout(int)}; the value is in MILLISECONDS
     * 
     */
    SocketTimeout() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withSocketTimeout(Integer.parseInt(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withTcpKeepAlive(boolean)}.
     * 
     */
    TcpKeepAlive() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withTcpKeepAlive(BooleanUtils.toBoolean(str));
      }
    },
    /**
     * Invokes {@link ClientConfiguration#withUserAgent(String)}
     * 
     */
    UserAgent() {
      @Override
      ClientConfiguration configure(ClientConfiguration cc, String str) {
        return cc.withUserAgent(str);
      }
    };
    abstract ClientConfiguration configure(ClientConfiguration cc, String str) throws Exception;
  };

  @NotNull
  @AutoPopulated
  @Valid
  private KeyValuePairSet clientConfigurationProperties;

  @AdvancedConfig
  @Valid
  private RetryPolicyBuilder retryPolicyBuilder;


  public AdvancedSQSImplementation() {
    setClientConfigurationProperties(new KeyValuePairSet());
  }

  @Override
  public ConnectionFactory createConnectionFactory() throws JMSException {
    SQSConnectionFactory connectionFactory = null;
    try {
      connectionFactory = configure(builder()).build();
    } catch (PasswordException e) {
      rethrowJMSException(e);
    } catch (Exception e) {
      rethrowJMSException("Exception configuring client configuration", e);
    }

    return connectionFactory;
  }

  Builder configure(Builder builder) throws Exception {
    ClientConfiguration cc = builder.getClientConfiguration();
    for (KeyValuePair kvp : getClientConfigurationProperties().getKeyValuePairs()) {
      boolean matched = false;
      for (ClientConfigurationProperties ccp : ClientConfigurationProperties.values()) {
        if (kvp.getKey().equalsIgnoreCase(ccp.toString())) {
          ccp.configure(cc, kvp.getValue());
          matched = true;
          break;
        }
      }
      if (!matched) {
        log.trace("Ignoring unsupported Property " + kvp.getKey());
      }
    }
    if (getRetryPolicyBuilder() != null) {
      cc = cc.withRetryPolicy(getRetryPolicyBuilder().build());
    }
    builder.setClientConfiguration(cc);
    return builder;
  }

  public KeyValuePairSet getClientConfigurationProperties() {
    return clientConfigurationProperties;
  }

  public void setClientConfigurationProperties(KeyValuePairSet kvps) {
    this.clientConfigurationProperties = Args.notNull(kvps, "clientConfigurationProperties");
  }

  public RetryPolicyBuilder getRetryPolicyBuilder() {
    return retryPolicyBuilder;
  }

  /**
   * Set the builder for the {@link com.amazonaws.retry.RetryPolicy}.
   * 
   * @param b the builder.
   */
  public void setRetryPolicyBuilder(RetryPolicyBuilder b) {
    this.retryPolicyBuilder = b;
  }


}
