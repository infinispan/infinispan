package org.infinispan.cli.connection;

/**
 *
 * Connector.
 *
 * @author tst
 * @since 5.2
 */
public interface Connector {
   Connection getConnection(String connectionString);
}
