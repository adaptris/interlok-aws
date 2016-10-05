package com.adaptris.aws;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.ClientConfiguration;

public class ClientConfigurationBuilder {

  private static transient Logger log = LoggerFactory.getLogger(ClientConfigurationBuilder.class);

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
        // Special case for handling proxies; we know it defaults to "null"; so if
        // someone configures proxyHost="" then we don't need to do anything.
        if (!isBlank(str)) {
          return cc.withProxyHost(str);
        }
        return cc;
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
        // Special case for handling proxies; we know it defaults to -1; so if
        // someone configures proxyPort="" then we don't need to do anything.
        if (!isBlank(str)) {
          return cc.withProxyPort(Integer.parseInt(str));
        }
        return cc;
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

  public static ClientConfiguration build(KeyValuePairSet kvps) throws Exception {
    return configure(new ClientConfiguration(), kvps);
  }

  public static ClientConfiguration configure(ClientConfiguration clientConfig, KeyValuePairSet settings) throws Exception {
    ClientConfiguration cfg = clientConfig;
    for (KeyValuePair kvp : settings.getKeyValuePairs()) {
      boolean matched = false;
      for (ClientConfigurationProperties ccp : ClientConfigurationProperties.values()) {
        if (kvp.getKey().equalsIgnoreCase(ccp.toString())) {
          cfg = ccp.configure(cfg, kvp.getValue());
          matched = true;
          break;
        }
      }
      if (!matched) {
        log.trace("Ignoring unsupported Property " + kvp.getKey());
      }
    }
    return cfg;
  }
}

