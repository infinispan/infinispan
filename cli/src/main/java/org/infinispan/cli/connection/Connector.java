package org.infinispan.cli.connection;

import java.util.Properties;

import org.infinispan.cli.impl.SSLContextSettings;

/**
 * Connector.
 *
 * @author tst
 * @since 5.2
 */
public interface Connector {
   Connection getConnection(Properties properties, String connectionString, SSLContextSettings sslContext);
}
