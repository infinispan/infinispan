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

import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.infinispan.util.TypedProperties;

@BuiltBy(JdbcStringBasedCacheStoreConfigurationBuilder.class)
public class JdbcStringBasedCacheStoreConfiguration extends AbstractJdbcCacheStoreConfiguration implements LegacyLoaderAdapter<JdbcStringBasedCacheStoreConfig>{

   private final String key2StringMapper;
   private final TableManipulationConfiguration table;

   protected JdbcStringBasedCacheStoreConfiguration(String key2StringMapper, TableManipulationConfiguration table,
         String driverClass, String connectionUrl, String userName, String password, String connectionFactoryClass,
         String datasourceJndiLocation, long lockAcquistionTimeout, int lockConcurrencyLevel, boolean purgeOnStartup,
         boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications,
         TypedProperties properties, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(driverClass, connectionUrl, userName, password, connectionFactoryClass, datasourceJndiLocation,
            lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads,
            fetchPersistentState, ignoreModifications, properties, async, singletonStore);
      this.key2StringMapper = key2StringMapper;
      this.table = table;
   }

   public String key2StringMapper() {
      return key2StringMapper;
   }

   public TableManipulationConfiguration table() {
      return table;
   }

   @Override
   public JdbcStringBasedCacheStoreConfig adapt() {
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig();

      // StoreConfiguration
      LegacyConfigurationAdaptor.adapt(this, config);

      // AbstractJdbcCacheStoreConfiguration
      config.setConnectionFactoryClass(connectionFactoryClass());
      config.setConnectionUrl(connectionUrl());
      config.setDatasourceJndiLocation(datasource());
      config.setDriverClass(driverClass());
      config.setUserName(userName());
      config.setPassword(password());

      // JdbcStringBasedCacheStoreConfiguration
      config.setKey2StringMapperClass(key2StringMapper);

      // TableManipulation
      config.setCreateTableOnStart(table.createOnStart());
      config.setDropTableOnExit(table.dropOnExit());
      config.setBatchSize(table.batchSize());
      config.setFetchSize(table.fetchSize());
      config.setCacheName(table.cacheName());
      config.setDataColumnName(table.dataColumnName());
      config.setDataColumnType(table.dataColumnType());
      config.setIdColumnName(table.idColumnName());
      config.setIdColumnType(table.idColumnType());
      config.setTimestampColumnName(table.timestampColumnName());
      config.setTimestampColumnType(table.timestampColumnType());
      config.setTableNamePrefix(table.tableNamePrefix());

      return config ;
   }

   @Override
   public String toString() {
      return "JdbcStringBasedCacheStoreConfiguration [key2StringMapper=" + key2StringMapper + ", table=" + table
            + ", driverClass()=" + driverClass() + ", connectionUrl()=" + connectionUrl() + ", userName()="
            + userName() + ", password()=" + password() + ", connectionFactoryClass()=" + connectionFactoryClass()
            + ", datasourceJndiLocation()=" + datasource() + ", lockAcquistionTimeout()="
            + lockAcquistionTimeout() + ", lockConcurrencyLevel()=" + lockConcurrencyLevel() + ", async()=" + async()
            + ", singletonStore()=" + singletonStore() + ", purgeOnStartup()=" + purgeOnStartup()
            + ", purgeSynchronously()=" + purgeSynchronously() + ", purgerThreads()=" + purgerThreads()
            + ", fetchPersistentState()=" + fetchPersistentState() + ", ignoreModifications()=" + ignoreModifications()
            + ", properties()=" + properties() + "]";
   }

}