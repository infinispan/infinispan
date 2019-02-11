/**
 * This package contains hacked SSL Engine for handling ALPN.
 *
 * <p>
 *    Unfortunately JDK 8 doesn't have support handling TLS/ALPN for negotiating protocols. Using OpenSSL doesn't
 *    improve the situation neither since we obtain an initialized {@link javax.net.ssl.SSLContext} from Wildfly.
 *    There's also 3rd element of the puzzle - Netty, which contains its own class hierarchy for handling TLS.
 *
 *    So in order to support TLS/ALPN without Jetty's agent (which requires specific version per JVM version), we need
 *    to wrap original {@link javax.net.ssl.SSLEngine} with {@link io.netty.handler.ssl.ALPNHackSSLEngine} (that
 *    supports ALPN) and then wrap it again in {@link io.netty.handler.ssl.AlpnHackedJdkSslEngine} (and friends) to
 *    pass it to Netty.
 *
 *    All classes in this package can be removed once we are baselined on JDK9.
 * </p>
 */
package io.netty.handler.ssl;
