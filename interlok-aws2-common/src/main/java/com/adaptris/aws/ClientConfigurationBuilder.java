/*
    Copyright 2018 Adaptris

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.aws;

import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;

public class ClientConfigurationBuilder {

  private static transient Logger log = LoggerFactory.getLogger(ClientConfigurationBuilder.class);
//
//  public enum ClientConfigurationProperties {
//    /**
//     * Invokes {@link ClientConfiguration#withConnectionTimeout(int)}; the value is in MILLISECONDS
//     *
//     */
//    ConnectionTimeout() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withConnectionTimeout(Integer.parseInt(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withConnectionTTL(long)}; the value is in MILLISECONDS
//     *
//     */
//    ConnectionTTL() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withConnectionTTL(Long.parseLong(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withGzip(boolean)}
//     */
//    Gzip() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withGzip(BooleanUtils.toBoolean(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withLocalAddress(InetAddress)} using {@link InetAddress#getByName(String)}
//     *
//     */
//    LocalAddress() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) throws UnknownHostException {
//        return cc.withLocalAddress(InetAddress.getByName(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withMaxConnections(int)}
//     *
//     */
//    MaxConnections() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withMaxConnections(Integer.parseInt(str));
//      }
//    },
//
//    /**
//     * Invokes {@link ClientConfiguration#withMaxErrorRetry(int)}
//     *
//     */
//    MaxErrorRetry() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withMaxErrorRetry(Integer.parseInt(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withPreemptiveBasicProxyAuth(boolean)}
//     *
//     */
//    PreemptiveBasicProxyAuth() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withPreemptiveBasicProxyAuth(BooleanUtils.toBoolean(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withProtocol(com.amazonaws.Protocol)}
//     *
//     */
//    Protocol() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withProtocol(com.amazonaws.Protocol.valueOf(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withProxyDomain(String)}
//     *
//     */
//    ProxyDomain() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withProxyDomain(str);
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withProxyHost(String)}
//     *
//     */
//    ProxyHost() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        // Special case for handling proxies; we know it defaults to "null"; so if
//        // someone configures proxyHost="" then we don't need to do anything.
//        if (!isBlank(str)) {
//          return cc.withProxyHost(str);
//        }
//        return cc;
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withProxyPassword(String)}
//     *
//     */
//    ProxyPassword() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withProxyPassword(str);
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withProxyPort(int)}
//     *
//     */
//    ProxyPort() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        // Special case for handling proxies; we know it defaults to -1; so if
//        // someone configures proxyPort="" then we don't need to do anything.
//        if (!isBlank(str)) {
//          return cc.withProxyPort(Integer.parseInt(str));
//        }
//        return cc;
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withProxyUsername(String)}
//     *
//     */
//    ProxyUsername() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withProxyUsername(str);
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withProxyWorkstation(String)}
//     *
//     */
//    ProxyWorkstation() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withProxyWorkstation(str);
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withResponseMetadataCacheSize(int)}
//     *
//     */
//    ResponseMetadataCacheSize() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withResponseMetadataCacheSize(Integer.parseInt(str));
//      }
//    },
//
//    /**
//     * Invokes {@link ClientConfiguration#withReaper(boolean)}
//     *
//     */
//    Reaper() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withReaper(BooleanUtils.toBoolean(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withSignerOverride(String)}
//     *
//     */
//    SignerOverride() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withSignerOverride(str);
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withSocketBufferSizeHints(int, int)} after parsing a comma separated string.
//     *
//     */
//    SocketBufferSizeHints() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        String[] split = str.split(",");
//        return cc.withSocketBufferSizeHints(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withSocketTimeout(int)}; the value is in MILLISECONDS
//     *
//     */
//    SocketTimeout() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withSocketTimeout(Integer.parseInt(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withTcpKeepAlive(boolean)}.
//     *
//     */
//    TcpKeepAlive() {
//      @Override
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withTcpKeepAlive(BooleanUtils.toBoolean(str));
//      }
//    },
//    /**
//     * Invokes {@link ClientConfiguration#withUserAgent(String)}
//     *
//     */
//    UserAgent() {
//      @Override
//      @SuppressWarnings("deprecation")
//      ClientConfiguration configure(ClientConfiguration cc, String str) {
//        return cc.withUserAgent(str);
//      }
//    };
//    abstract ClientConfiguration configure(ClientConfiguration cc, String str) throws Exception;
//  };
//
  public static ClientOverrideConfiguration build(KeyValuePairSet settings) throws Exception {
    return build(settings, new DefaultRetryPolicyFactory());
  }

  public static ClientOverrideConfiguration build(KeyValuePairSet settings, RetryPolicyFactory b) throws Exception {
    return configure(settings, b.build());
  }

  public static ClientOverrideConfiguration configure(KeyValuePairSet settings, RetryPolicy retry) throws Exception
  {
    ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();
    builder.retryPolicy(retry);
    for (KeyValuePair kvp : settings.getKeyValuePairs())
    {
      String key = kvp.getKey();
      String value = kvp.getValue();

      builder.putHeader(key, value);
    }
    return builder.build();
  }
}

