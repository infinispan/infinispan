package org.infinispan.cdc.internal.configuration;

import java.sql.Connection;
import java.sql.SQLException;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;
import org.infinispan.cdc.internal.url.DatabaseURL;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.SimpleConnectionFactory;

public record ConnectionParameters(String username, String password, DatabaseURL url) {

   public static ConnectionParameters create(ConnectionFactory factory) throws SQLException {
      if (factory instanceof SimpleConnectionFactory scf)
         return create(scf);

      if (factory instanceof PooledConnectionFactory pcf)
         return create(pcf);

      throw new IllegalArgumentException("Unknown connection factory: " + factory.getClass());
   }

   private static ConnectionParameters create(SimpleConnectionFactory scf) throws SQLException {
      String username = scf.getUserName();
      String password = scf.getPassword();
      DatabaseURL url = getDatabaseUrl(scf, scf.getConnectionUrl());
      return new ConnectionParameters(username, password, url);
   }

   private static ConnectionParameters create(PooledConnectionFactory pcf) throws SQLException {
      String username = pcf.username();
      String password = pcf.password();
      DatabaseURL url = getDatabaseUrl(pcf, pcf.jdbcConnectionUrl());
      return new ConnectionParameters(username, password, url);
   }

   private static DatabaseURL getDatabaseUrl(ConnectionFactory factory, String jdbcUrl) throws SQLException {
      DatabaseURL url = DatabaseURL.create(jdbcUrl);

      return url;
   }

   private static DatabaseURL databaseUrlAdapter(DatabaseURL delegate, String schema) {
      return new DatabaseURL() {
         @Override
         public String host() {
            return delegate.host();
         }

         @Override
         public String port() {
            return delegate.port();
         }

         @Override
         public String database() {
            return schema;
         }

         @Override
         public DatabaseVendor vendor() {
            return delegate.vendor();
         }

         @Override
         public String toString() {
            return delegate.toString() + " - schema=" + schema;
         }
      };
   }

   private static String retrieveConnectedDatabase(ConnectionFactory factory) throws SQLException {
      try (Connection conn = factory.getConnection()) {
         return conn.getSchema();
      }
   }
}
