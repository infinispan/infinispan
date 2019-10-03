package org.infinispan.cli.connection;

import org.infinispan.cli.impl.SSLContextSettings;

/**
 * Connector.
 *
 * @author tst
 * @since 5.2
 */
public interface Connector {
   Connection getConnection(String connectionString, SSLContextSettings sslContext);
}
