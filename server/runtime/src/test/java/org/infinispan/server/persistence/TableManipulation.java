package org.infinispan.server.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.jdbc.impl.table.TableManagerFactory;

public class TableManipulation {

   private String cacheName;
   private ConnectionFactory connectionFactory;
   private Connection connection;
   private PooledConnectionFactoryConfigurationBuilder persistenceConfiguration;
   private ConfigurationBuilder configurationBuilder;

   public TableManipulation(String cacheName, PooledConnectionFactoryConfigurationBuilder persistenceConfiguration, ConfigurationBuilder configurationBuilder) {
      this.cacheName = cacheName;
      this.persistenceConfiguration = persistenceConfiguration;
      this.configurationBuilder = configurationBuilder;
   }

   private ConnectionFactory getConnectionFactory() {
      PooledConnectionFactoryConfiguration pooledConnectionFactoryConfiguration = persistenceConfiguration.create();
      connectionFactory = ConnectionFactory.getConnectionFactory(PooledConnectionFactory.class);
      connectionFactory.start(pooledConnectionFactoryConfiguration, connectionFactory.getClass().getClassLoader());
      return connectionFactory;
   }

   private TableManager getTableManager() {
      JdbcStringBasedStoreConfigurationBuilder store = (JdbcStringBasedStoreConfigurationBuilder) configurationBuilder.persistence().stores().get(0);
      return TableManagerFactory.getManager(getConnectionFactory(), store.create(), cacheName);
   }

   public String getValueByKey(String key) throws Exception{
      if(connection == null) {
         connection = getConnectionFactory().getConnection();
      }
      TableManager tableManager = getTableManager();
      PreparedStatement ps = connection.prepareStatement(tableManager.getSelectIdRowSqlWithLike());
      ps.setString(1, "%" + key + "%");
      ResultSet rs = ps.executeQuery();
      if(rs.next()) {
         return rs.getString("ID");
      }
      return null;
   }

   public int countAllRows() throws Exception {
      TableManager tableManager = getTableManager();
      Connection connection = getConnectionFactory().getConnection();
      PreparedStatement ps = connection.prepareStatement(tableManager.getCountRowsSql());
      ResultSet rs = ps.executeQuery();
      if(rs.next()) {
         return rs.getInt(1);
      }
      return 0;
   }

   public boolean tableExists() {
      return getTableManager().tableExists(getConnectionFactory().getConnection());
   }

}
