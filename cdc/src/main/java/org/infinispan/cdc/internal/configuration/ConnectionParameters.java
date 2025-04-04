package org.infinispan.cdc.internal.configuration;

import java.sql.SQLException;
import java.util.Collection;

import javax.sql.DataSource;

import org.infinispan.cdc.internal.url.DatabaseURL;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.CDIConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.SimpleConnectionFactory;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.security.SimplePassword;

public record ConnectionParameters(String username, String password, DatabaseURL url) {

   public static ConnectionParameters create(ConnectionFactory factory) throws SQLException {
      if (factory instanceof SimpleConnectionFactory scf)
         return create(scf);

      if (factory instanceof PooledConnectionFactory pcf)
         return create(pcf);

      if (factory instanceof ManagedConnectionFactory mcf)
         return create(mcf.dataSource());

      if (factory instanceof CDIConnectionFactory ccf)
         return create(ccf.dataSource());

      throw new IllegalArgumentException("Unknown connection factory: " + factory.getClass());
   }

   private static ConnectionParameters create(SimpleConnectionFactory scf) throws SQLException {
      String username = scf.getUserName();
      String password = scf.getPassword();
      DatabaseURL url = DatabaseURL.create(scf.getConnectionUrl());
      return new ConnectionParameters(username, password, url);
   }

   private static ConnectionParameters create(PooledConnectionFactory pcf) throws SQLException {
      String username = pcf.username();
      String password = pcf.password();
      DatabaseURL url = DatabaseURL.create(pcf.jdbcConnectionUrl());
      return new ConnectionParameters(username, password, url);
   }

   private static ConnectionParameters create(DataSource dataSource) throws SQLException {
      if (!(dataSource instanceof AgroalDataSource ds))
         throw new IllegalArgumentException("Configured data source type is not supported: " + dataSource.getClass());

      String username = ds.getConfiguration().connectionPoolConfiguration()
            .connectionFactoryConfiguration()
            .principal().getName();
      String password = readDataSourcePassword(ds);
      String jdbcUrl = ds.getConfiguration().connectionPoolConfiguration()
            .connectionFactoryConfiguration()
            .jdbcUrl();
      DatabaseURL url = DatabaseURL.create(jdbcUrl);
      return new ConnectionParameters(username, password, url);
   }

   private static String readDataSourcePassword(AgroalDataSource ds) {
      Collection<Object> credentials = ds.getConfiguration().connectionPoolConfiguration()
            .connectionFactoryConfiguration()
            .credentials();

      if (credentials.size() != 1)
         throw new IllegalStateException("Unable to recover database credentials");

      SimplePassword sp = (SimplePassword) credentials.iterator().next();
      return sp.getWord();
   }
}
