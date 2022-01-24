/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.testcontainers.opensearch;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

/**
 * The Opensearch Docker container (single node cluster) which exposes by default ports 9200 (http)
 * and 9300 (tcp, deprecated).
 */
public class OpensearchContainer extends GenericContainer<OpensearchContainer> {
    // Default username to connect to Opensearch instance
    private static final String DEFAULT_USER = "admin";
    // Default password to connect to Opensearch instance
    private static final String DEFAULT_PASSWORD = "admin";

    // Default HTTP port.
    private static final int DEFAULT_HTTP_PORT = 9200;

    // Default TCP port (deprecated and may be removed in future versions).
    @Deprecated private static final int DEFAULT_TCP_PORT = 9300;

    // Opensearch Docker base image.
    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("opensearchproject/opensearch");

    // See please https://github.com/testcontainers/testcontainers-java/pull/4951
    static {
        // Create a trust manager that does not validate certificate chains
        // and trust all certificates
        final TrustManager[] trustAllCerts =
                new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }

                        @Override
                        public void checkClientTrusted(
                                final X509Certificate[] certs, final String authType) {}

                        @Override
                        public void checkServerTrusted(
                                final X509Certificate[] certs, final String authType) {}
                    }
                };

        try {
            // Create custom SSL context and set the "trust all certificates" trust manager
            final SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(new KeyManager[0], trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (final NoSuchAlgorithmException | KeyManagementException ex) {
            throw new RuntimeException("Unable to create custom SSL factory instance", ex);
        }
    }

    /**
     * Create an Opensearch Container by passing the full docker image name.
     *
     * @param dockerImageName Full docker image name as a {@link String}, like:
     *     opensearchproject/opensearch:1.2.4
     */
    public OpensearchContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    /**
     * Create an Opensearch Container by passing the full docker image name.
     *
     * @param dockerImageName Full docker image name as a {@link DockerImageName}, like:
     *     DockerImageName.parse("opensearchproject/opensearch:1.2.4")
     */
    public OpensearchContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        withNetworkAliases("opensearch-" + Base58.randomString(6));
        withEnv("discovery.type", "single-node");
        addExposedPorts(DEFAULT_HTTP_PORT, DEFAULT_TCP_PORT);
        setWaitStrategy(
                new HttpWaitStrategy()
                        .forPort(DEFAULT_HTTP_PORT)
                        .withBasicCredentials(DEFAULT_USER, DEFAULT_PASSWORD)
                        .usingTls() /* Opensearch uses HTTPS by default */
                        .forStatusCodeMatching(
                                response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                        .withReadTimeout(Duration.ofSeconds(10))
                        .withStartupTimeout(Duration.ofMinutes(5)));
    }

    /**
     * Return HTTP host and port to connect to Opensearch container.
     *
     * @return HTTP host and port (in a form of "host:port")
     */
    public String getHttpHostAddress() {
        return "https://" + getHost() + ":" + getMappedPort(DEFAULT_HTTP_PORT);
    }

    /**
     * Return socket address to connect to Opensearch over TCP. The TransportClient will is
     * deprecated and may be removed in future versions.
     *
     * @return TCP socket address
     */
    @Deprecated
    public InetSocketAddress getTcpHost() {
        return new InetSocketAddress(getHost(), getMappedPort(DEFAULT_TCP_PORT));
    }

    public String getUsername() {
        return DEFAULT_USER;
    }

    public String getPassword() {
        return DEFAULT_PASSWORD;
    }
}
