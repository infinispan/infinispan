/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.jdbc.configuration;

import org.h2.Driver;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.jdbc.DatabaseType;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStoreConfig;
import org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStoreConfig;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.jdbc.configuration.ConfigurationTest")
public class ConfigurationTest {

   private static final String JDBC_URL = "jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1";

   public void testImplicitPooledConnectionFactory() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(JdbcBinaryCacheStoreConfigurationBuilder.class)
         .connectionPool().connectionUrl(JDBC_URL);
      Configuration configuration = b.build();
      JdbcBinaryCacheStoreConfiguration store = (JdbcBinaryCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
   }

   public void testImplicitManagedConnectionFactory() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(JdbcBinaryCacheStoreConfigurationBuilder.class)
         .dataSource().jndiUrl("java:jboss/datasources/ExampleDS");
      Configuration configuration = b.build();
      JdbcBinaryCacheStoreConfiguration store = (JdbcBinaryCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.connectionFactory() instanceof ManagedConnectionFactoryConfiguration;
   }

   public void testJdbcBinaryCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(JdbcBinaryCacheStoreConfigurationBuilder.class)
         .connectionPool()
            .connectionUrl(JDBC_URL)
            .username("dbuser")
            .password("dbpass")
            .driverClass(Driver.class)
         .fetchPersistentState(true)
         .lockConcurrencyLevel(32)
         .table()
            .tableNamePrefix("BINARY_")
            .idColumnName("id").idColumnType("VARCHAR")
            .dataColumnName("datum").dataColumnType("BINARY")
            .timestampColumnName("version").timestampColumnType("BIGINT")
         .async().enable();
      Configuration configuration = b.build();
      JdbcBinaryCacheStoreConfiguration store = (JdbcBinaryCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store.table().tableNamePrefix().equals("BINARY_");
      assert store.table().idColumnName().equals("id");
      assert store.table().idColumnType().equals("VARCHAR");
      assert store.table().dataColumnName().equals("datum");
      assert store.table().dataColumnType().equals("BINARY");
      assert store.table().timestampColumnName().equals("version");
      assert store.table().timestampColumnType().equals("BIGINT");
      assert store.fetchPersistentState();
      assert store.lockConcurrencyLevel() == 32;
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(JdbcBinaryCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbcBinaryCacheStoreConfiguration store2 = (JdbcBinaryCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store2.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store2.table().tableNamePrefix().equals("BINARY_");
      assert store2.table().idColumnName().equals("id");
      assert store2.table().idColumnType().equals("VARCHAR");
      assert store2.table().dataColumnName().equals("datum");
      assert store2.table().dataColumnType().equals("BINARY");
      assert store2.table().timestampColumnName().equals("version");
      assert store2.table().timestampColumnType().equals("BIGINT");
      assert store2.fetchPersistentState();
      assert store2.lockConcurrencyLevel() == 32;
      assert store2.async().enabled();

      JdbcBinaryCacheStoreConfig legacy = store.adapt();
      assert legacy.getConnectionFactoryConfig().getConnectionUrl().equals(JDBC_URL);
      assert legacy.getTableManipulation().getTableNamePrefix().equals("BINARY_");
      assert legacy.getTableManipulation().getIdColumnName().equals("id");
      assert legacy.getTableManipulation().getIdColumnType().equals("VARCHAR");
      assert legacy.getTableManipulation().getDataColumnName().equals("datum");
      assert legacy.getTableManipulation().getDataColumnType().equals("BINARY");
      assert legacy.getTableManipulation().getTimestampColumnName().equals("version");
      assert legacy.getTableManipulation().getTimestampColumnType().equals("BIGINT");
      assert legacy.isFetchPersistentState();
      assert legacy.getLockConcurrencyLevel() == 32;
      assert legacy.getAsyncStoreConfig().isEnabled();
   }

   public void testJdbcMixedCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      JdbcMixedCacheStoreConfigurationBuilder mixedBuilder = b.loaders().addStore(JdbcMixedCacheStoreConfigurationBuilder.class)
         .connectionPool().connectionUrl(JDBC_URL)
         .fetchPersistentState(true)
         .lockConcurrencyLevel(32)
         .fetchSize(50)
         .batchSize(50)
         .databaseType(DatabaseType.H2);
      mixedBuilder.async().enable();

      mixedBuilder.binaryTable()
         .tableNamePrefix("BINARY_")
         .idColumnName("id").idColumnType("VARCHAR")
         .dataColumnName("datum").dataColumnType("BINARY")
         .timestampColumnName("version").timestampColumnType("BIGINT");

      mixedBuilder.stringTable()
         .tableNamePrefix("STRINGS_")
         .idColumnName("id").idColumnType("VARCHAR")
         .dataColumnName("datum").dataColumnType("BINARY")
         .timestampColumnName("version").timestampColumnType("BIGINT");

      Configuration configuration = b.build();
      JdbcMixedCacheStoreConfiguration store = (JdbcMixedCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store.binaryTable().tableNamePrefix().equals("BINARY_");
      assert store.binaryTable().idColumnName().equals("id");
      assert store.binaryTable().idColumnType().equals("VARCHAR");
      assert store.binaryTable().dataColumnName().equals("datum");
      assert store.binaryTable().dataColumnType().equals("BINARY");
      assert store.binaryTable().timestampColumnName().equals("version");
      assert store.binaryTable().timestampColumnType().equals("BIGINT");
      assert store.stringTable().tableNamePrefix().equals("STRINGS_");
      assert store.stringTable().idColumnName().equals("id");
      assert store.stringTable().idColumnType().equals("VARCHAR");
      assert store.stringTable().dataColumnName().equals("datum");
      assert store.stringTable().dataColumnType().equals("BINARY");
      assert store.stringTable().timestampColumnName().equals("version");
      assert store.stringTable().timestampColumnType().equals("BIGINT");
      assert store.batchSize() == 50;
      assert store.fetchSize() == 50;
      assert store.databaseType() == DatabaseType.H2;
      assert store.fetchPersistentState();
      assert store.lockConcurrencyLevel() == 32;
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(JdbcMixedCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbcMixedCacheStoreConfiguration store2 = (JdbcMixedCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store2.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store2.binaryTable().idColumnName().equals("id");
      assert store2.binaryTable().idColumnType().equals("VARCHAR");
      assert store2.binaryTable().dataColumnName().equals("datum");
      assert store2.binaryTable().dataColumnType().equals("BINARY");
      assert store2.binaryTable().timestampColumnName().equals("version");
      assert store2.binaryTable().timestampColumnType().equals("BIGINT");
      assert store2.stringTable().tableNamePrefix().equals("STRINGS_");
      assert store2.stringTable().idColumnName().equals("id");
      assert store2.stringTable().idColumnType().equals("VARCHAR");
      assert store2.stringTable().dataColumnName().equals("datum");
      assert store2.stringTable().dataColumnType().equals("BINARY");
      assert store2.stringTable().timestampColumnName().equals("version");
      assert store2.stringTable().timestampColumnType().equals("BIGINT");
      assert store2.fetchPersistentState();
      assert store2.lockConcurrencyLevel() == 32;
      assert store2.async().enabled();

      JdbcMixedCacheStoreConfig legacy = store.adapt();
      assert legacy.getConnectionFactoryConfig().getConnectionUrl().equals(JDBC_URL);
      assert legacy.isFetchPersistentState();
      assert legacy.getLockConcurrencyLevel() == 32;
      assert legacy.getAsyncStoreConfig().isEnabled();
   }

   public void testJdbcStringCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(JdbcStringBasedCacheStoreConfigurationBuilder.class)
         .connectionPool()
            .connectionUrl(JDBC_URL)
         .fetchPersistentState(true)
         .lockConcurrencyLevel(32)
         .table()
            .tableNamePrefix("STRINGS_")
            .idColumnName("id").idColumnType("VARCHAR")
            .dataColumnName("datum").dataColumnType("BINARY")
            .timestampColumnName("version").timestampColumnType("BIGINT")
         .async().enable();
      Configuration configuration = b.build();
      JdbcStringBasedCacheStoreConfiguration store = (JdbcStringBasedCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store.table().tableNamePrefix().equals("STRINGS_");
      assert store.table().idColumnName().equals("id");
      assert store.table().idColumnType().equals("VARCHAR");
      assert store.table().dataColumnName().equals("datum");
      assert store.table().dataColumnType().equals("BINARY");
      assert store.table().timestampColumnName().equals("version");
      assert store.table().timestampColumnType().equals("BIGINT");
      assert store.fetchPersistentState();
      assert store.lockConcurrencyLevel() == 32;
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(JdbcStringBasedCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbcStringBasedCacheStoreConfiguration store2 = (JdbcStringBasedCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store2.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store2.table().tableNamePrefix().equals("STRINGS_");
      assert store2.table().idColumnName().equals("id");
      assert store2.table().idColumnType().equals("VARCHAR");
      assert store2.table().dataColumnName().equals("datum");
      assert store2.table().dataColumnType().equals("BINARY");
      assert store2.table().timestampColumnName().equals("version");
      assert store2.table().timestampColumnType().equals("BIGINT");
      assert store2.fetchPersistentState();
      assert store2.lockConcurrencyLevel() == 32;
      assert store2.async().enabled();

      JdbcStringBasedCacheStoreConfig legacy = store.adapt();
      assert legacy.getConnectionFactoryConfig().getConnectionUrl().equals(JDBC_URL);
      assert legacy.getTableManipulation().getTableNamePrefix().equals("STRINGS_");
      assert legacy.getTableManipulation().getIdColumnName().equals("id");
      assert legacy.getTableManipulation().getIdColumnType().equals("VARCHAR");
      assert legacy.getTableManipulation().getDataColumnName().equals("datum");
      assert legacy.getTableManipulation().getDataColumnType().equals("BINARY");
      assert legacy.getTableManipulation().getTimestampColumnName().equals("version");
      assert legacy.getTableManipulation().getTimestampColumnType().equals("BIGINT");
      assert legacy.isFetchPersistentState();
      assert legacy.getLockConcurrencyLevel() == 32;
      assert legacy.getAsyncStoreConfig().isEnabled();
   }
}