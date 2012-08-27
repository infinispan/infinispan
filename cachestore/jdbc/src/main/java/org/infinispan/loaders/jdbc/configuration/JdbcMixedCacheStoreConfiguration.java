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
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStoreConfig;
import org.infinispan.util.TypedProperties;

/**
 *
 * JdbcMixedCacheStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(JdbcMixedCacheStoreConfigurationBuilder.class)
public class JdbcMixedCacheStoreConfiguration extends AbstractJdbcCacheStoreConfiguration implements
      LegacyLoaderAdapter<JdbcMixedCacheStoreConfig> {

   private final TableManipulationConfiguration binaryTable;
   private final TableManipulationConfiguration stringTable;
   private final String key2StringMapper;

   protected JdbcMixedCacheStoreConfiguration(String key2StringMapper, TableManipulationConfiguration binaryTable,
         TableManipulationConfiguration stringTable, String driverClass, String connectionUrl, String userName,
         String password, String connectionFactoryClass, String datasourceJndiLocation, long lockAcquistionTimeout,
         int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
         boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(driverClass, connectionUrl, userName, password, connectionFactoryClass, datasourceJndiLocation,
            lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads,
            fetchPersistentState, ignoreModifications, properties, async, singletonStore);
      this.key2StringMapper = key2StringMapper;
      this.binaryTable = binaryTable;
      this.stringTable = stringTable;
   }

   public String key2StringMapper() {
      return key2StringMapper;
   }

   public TableManipulationConfiguration binaryTable() {
      return binaryTable;
   }

   public TableManipulationConfiguration stringTable() {
      return stringTable;
   }

   @Override
   public JdbcMixedCacheStoreConfig adapt() {
      JdbcMixedCacheStoreConfig config = new JdbcMixedCacheStoreConfig();

      // StoreConfiguration
      config.fetchPersistentState(fetchPersistentState());
      config.ignoreModifications(ignoreModifications());
      config.purgeOnStartup(purgeOnStartup());
      config.purgeSynchronously(purgeSynchronously());
      config.purgerThreads(purgerThreads());

      // LockSupportCacheStoreConfiguration
      config.setLockAcquistionTimeout(lockAcquistionTimeout());
      config.setLockConcurrencyLevel(lockConcurrencyLevel());

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
      config.setCreateTableOnStartForBinary(binaryTable().createOnStart());
      config.setDropTableOnExitForBinary(binaryTable().dropOnExit());
      config.setTableNamePrefixForBinary(binaryTable().tableNamePrefix());
      config.setDataColumnNameForBinary(binaryTable().dataColumnName());
      config.setDataColumnTypeForBinary(binaryTable().dataColumnType());
      config.setIdColumnNameForBinary(binaryTable().idColumnName());
      config.setIdColumnTypeForBinary(binaryTable().idColumnType());
      config.setTimestampColumnNameForBinary(binaryTable().timestampColumnName());
      config.setTimestampColumnTypeForBinary(binaryTable().timestampColumnType());

      config.setCreateTableOnStartForStrings(stringTable().createOnStart());
      config.setDropTableOnExitForStrings(stringTable().dropOnExit());
      config.setTableNamePrefixForStrings(stringTable().tableNamePrefix());
      config.setDataColumnNameForStrings(stringTable().dataColumnName());
      config.setDataColumnTypeForStrings(stringTable().dataColumnType());
      config.setIdColumnNameForStrings(stringTable().idColumnName());
      config.setIdColumnTypeForStrings(stringTable().idColumnType());
      config.setTimestampColumnNameForStrings(stringTable().timestampColumnName());
      config.setTimestampColumnTypeForStrings(stringTable().timestampColumnType());

      return config;
   }

   @Override
   public String toString() {
      return "JdbcMixedCacheStoreConfiguration [binaryTable=" + binaryTable + ", stringTable=" + stringTable
            + ", key2StringMapper=" + key2StringMapper + ", getDriverClass()=" + driverClass()
            + ", getConnectionUrl()=" + connectionUrl() + ", getUserName()=" + userName() + ", getPassword()="
            + password() + ", getConnectionFactoryClass()=" + connectionFactoryClass()
            + ", getDatasourceJndiLocation()=" + datasource() + ", lockAcquistionTimeout()="
            + lockAcquistionTimeout() + ", lockConcurrencyLevel()=" + lockConcurrencyLevel() + ", async()=" + async()
            + ", singletonStore()=" + singletonStore() + ", purgeOnStartup()=" + purgeOnStartup()
            + ", purgeSynchronously()=" + purgeSynchronously() + ", purgerThreads()=" + purgerThreads()
            + ", fetchPersistentState()=" + fetchPersistentState() + ", ignoreModifications()=" + ignoreModifications()
            + ", properties()=" + properties() + "]";
   }

}