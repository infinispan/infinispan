package org.infinispan.persistence.jdbc.connectionfactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A simple interface that must be implemented by ConnectionPool wrapper classes.
 *
 * @author Ryan Emerson
 */
public interface ConnectionPool {
   void close();
   Connection getConnection() throws SQLException;
   int getMaxPoolSize();
   int getNumConnectionsAllUsers() throws SQLException;
   int getNumBusyConnectionsAllUsers() throws SQLException;
}
