/*
 * Copyright 2012-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.http;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.io.EmptyInputStream;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.internal.BaseAws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

/**
 * An {@link HttpRequestInterceptor} that signs requests using any AWS {@link Signer}
 * and {@link AwsCredentialsProvider}.
 */
// Since we use lombok, we abuse the Generated annotation to exclude this from jacoco (!)
@lombok.Generated
public class AWSRequestSigningApacheInterceptor implements HttpRequestInterceptor {
    /**
     * The service that we're connecting to. Technically not necessary.
     * Could be used by a future Signer, though.
     */
    private final String service;

    /**
     * The particular signer implementation.
     */
    private final BaseAws4Signer signer;

    /**
     * The source of AWS credentials for signing.
     */
    private final AwsCredentialsProvider awsCredentialsProvider;

    /**
     * Any parameters to bundle with the signing; during the presign stage.
     */
    private final Aws4PresignerParams presignParameters;

    /**
     *
     * @param service service that we're connecting to
     * @param signer particular signer implementation
     * @param awsCredentialsProvider source of AWS credentials for signing
     */
    public AWSRequestSigningApacheInterceptor(final String service,
                                final BaseAws4Signer signer,
                                final AwsCredentialsProvider awsCredentialsProvider,
                                final Aws4PresignerParams params) {
        this.service = service;
        this.signer = signer;
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.presignParameters = params;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(final HttpRequest request, final HttpContext context)
            throws HttpException, IOException {
        URIBuilder uriBuilder;
        try {
            uriBuilder = new URIBuilder(request.getRequestLine().getUri());
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URI" , e);
        }

        // Copy Apache HttpRequest to AWS DefaultRequest
        SdkHttpFullRequest.Builder signableRequestBuilder = SdkHttpFullRequest.builder();
//        signableRequestBuilder.service(service);

        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
        if (host != null) {
            signableRequestBuilder.uri(URI.create(host.toURI()));
        }
        final SdkHttpMethod httpMethod = SdkHttpMethod.fromValue(request.getRequestLine().getMethod());
        signableRequestBuilder.method(httpMethod);
//        try {
//            signableRequestBuilder.setResourcePath(uriBuilder.build().getRawPath());
//        } catch (URISyntaxException e) {
//            throw new IOException("Invalid URI" , e);
//        }
        if (request instanceof HttpEntityEnclosingRequest)
        {
            HttpEntityEnclosingRequest httpEntityEnclosingRequest = (HttpEntityEnclosingRequest)request;
            if (httpEntityEnclosingRequest.getEntity() != null)
            {
                signableRequestBuilder.contentStreamProvider(() ->
                {
                    try
                    {
                        return httpEntityEnclosingRequest.getEntity().getContent();
                    }
                    catch (IOException e)
                    {
                        return EmptyInputStream.INSTANCE;
                    }
                });
            }
        }
        signableRequestBuilder.rawQueryParameters(nvpToMapParams(uriBuilder.getQueryParams()));
        signableRequestBuilder.headers(headerArrayToMap(request.getAllHeaders()));


        SdkHttpFullRequest signableRequest = signableRequestBuilder.build();
        // Presign it
        if (presignParameters != null) {
            signer.presign(signableRequest, presignParameters);
        }
        // Sign it
        Aws4SignerParams.Builder signerParams = Aws4SignerParams.builder();
        signerParams.awsCredentials(awsCredentialsProvider.resolveCredentials());
        signer.sign(signableRequest, signerParams.build());


        // Now copy everything back
        request.setHeaders(mapToHeaderArray(signableRequestBuilder.headers()));
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest httpEntityEnclosingRequest =
                    (HttpEntityEnclosingRequest) request;
            if (httpEntityEnclosingRequest.getEntity() != null) {
                BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
                basicHttpEntity.setContent(signableRequestBuilder.contentStreamProvider().newStream());
                httpEntityEnclosingRequest.setEntity(basicHttpEntity);
            }
        }
    }

    /**
     *
     * @param params list of HTTP query params as NameValuePairs
     * @return a multimap of HTTP query params
     */
    private static Map<String, List<String>> nvpToMapParams(final List<NameValuePair> params) {
        Map<String, List<String>> parameterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (NameValuePair nvp : params) {
            List<String> argsList =
                    parameterMap.computeIfAbsent(nvp.getName(), k -> new ArrayList<>());
            argsList.add(nvp.getValue());
        }
        return parameterMap;
    }

    /**
     * @param headers modeled Header objects
     * @return a Map of header entries
     */
    private static Map<String, List<String>> headerArrayToMap(final Header[] headers) {
        Map<String, List<String>> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Header header : headers) {
            if (!skipHeader(header)) {
                headersMap.put(header.getName(), Arrays.asList(header.getValue()));
            }
        }
        return headersMap;
    }

    /**
     * @param header header line to check
     * @return true if the given header should be excluded when signing
     */
    private static boolean skipHeader(final Header header) {
        return ("content-length".equalsIgnoreCase(header.getName())
                && "0".equals(header.getValue())) // Strip Content-Length: 0
                || "host".equalsIgnoreCase(header.getName()); // Host comes from endpoint
    }

    /**
     * @param mapHeaders Map of header entries
     * @return modeled Header objects
     */
    private static Header[] mapToHeaderArray(final Map<String, List<String>> mapHeaders) {
        Header[] headers = new Header[mapHeaders.size()];
        int i = 0;
        for (Map.Entry<String, List<String>> headerEntry : mapHeaders.entrySet()) {
            headers[i++] = new BasicHeader(headerEntry.getKey(),
                    headerEntry.getValue().stream().map(n -> String.valueOf(n)).collect(Collectors.joining(","))
            );
        }
        return headers;
    }
}
