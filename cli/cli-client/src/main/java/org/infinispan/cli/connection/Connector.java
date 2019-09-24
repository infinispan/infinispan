package org.infinispan.cli.connection;

import javax.net.ssl.SSLContext;

/**
 * Connector.
 *
 * @author tst
 * @since 5.2
 */
public interface Connector {
   Connection getConnection(String connectionString, SSLContext sslContext);
}
