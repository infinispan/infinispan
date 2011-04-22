/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.jdbc.AbstractJdbcCacheStoreConfig;
import org.infinispan.loaders.jdbc.DatabaseType;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStoreConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;

/**
 * Configuration for {@link org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcMixedCacheStoreConfig extends AbstractJdbcCacheStoreConfig {

   private static final long serialVersionUID = -1343548133363285687L;

   private TableManipulation binaryTableManipulation = new TableManipulation();
   private TableManipulation stringsTableManipulation = new TableManipulation();
   private String key2StringMapper;
   private int binaryConcurrencyLevel = LockSupportCacheStoreConfig.DEFAULT_CONCURRENCY_LEVEL / 2;
   private int stringsConcurrencyLevel = LockSupportCacheStoreConfig.DEFAULT_CONCURRENCY_LEVEL / 2;
   private int lockAcquistionTimeout = LockSupportCacheStoreConfig.DEFAULT_LOCK_ACQUISITION_TIMEOUT;


   public JdbcMixedCacheStoreConfig(ConnectionFactoryConfig connectionFactoryConfig, TableManipulation binaryTableManipulation, TableManipulation stringsTableManipulation) {
      this.connectionFactoryConfig = connectionFactoryConfig;
      this.binaryTableManipulation = binaryTableManipulation;
      this.stringsTableManipulation = stringsTableManipulation;
   }

   public JdbcMixedCacheStoreConfig() {
      cacheLoaderClassName = JdbcMixedCacheStore.class.getName();
   }

   public void setConnectionFactoryConfig(ConnectionFactoryConfig connectionFactoryConfig) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig = connectionFactoryConfig;
   }

   public void setBinaryTableManipulation(TableManipulation binaryTableManipulation) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation = binaryTableManipulation;
   }

   public void setStringsTableManipulation(TableManipulation stringsTableManipulation) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation = stringsTableManipulation;
   }

   JdbcBinaryCacheStoreConfig getBinaryCacheStoreConfig() {
      JdbcBinaryCacheStoreConfig cacheStoreConfig = new JdbcBinaryCacheStoreConfig(false);
      cacheStoreConfig.setTableManipulation(binaryTableManipulation);
      cacheStoreConfig.setPurgeSynchronously(true);//just to make sure we don't create another thread
      cacheStoreConfig.setLockConcurrencyLevel(binaryConcurrencyLevel);
      cacheStoreConfig.setLockAcquistionTimeout(lockAcquistionTimeout);
      return cacheStoreConfig;
   }

   JdbcStringBasedCacheStoreConfig getStringCacheStoreConfig() {
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(false);
      config.setTableManipulation(stringsTableManipulation);
      config.setPurgeSynchronously(true); //just to make sure we don't create another thread
      config.setLockConcurrencyLevel(stringsConcurrencyLevel);
      config.setLockAcquistionTimeout(lockAcquistionTimeout);
      if (key2StringMapper != null) {
         config.setKey2StringMapperClass(key2StringMapper);
      }
      return config;
   }

   public void setIdColumnNameForStrings(String idColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      stringsTableManipulation.setIdColumnName(idColumnNameForStrings);
   }

   public void setIdColumnTypeForStrings(String idColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      stringsTableManipulation.setIdColumnType(idColumnTypeForStrings);
   }

   public void setTableNamePrefixForStrings(String tableNameForStrings) {
      testImmutability("stringsTableManipulation");
      if (tableNameForStrings == null) {
         throw new IllegalArgumentException("Null table name not allowed.");
      }
      if (tableNameForStrings.equals(binaryTableManipulation.getTableNamePrefix())) {
         throw new IllegalArgumentException("Same table name is used for both cache loaders, this is not allowed!");
      }
      stringsTableManipulation.setTableNamePrefix(tableNameForStrings);
   }

   public void setDataColumnNameForStrings(String dataColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      stringsTableManipulation.setDataColumnName(dataColumnNameForStrings);
   }

   public void setDataColumnTypeForStrings(String dataColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      stringsTableManipulation.setDataColumnType(dataColumnTypeForStrings);
   }

   public void setTimestampColumnNameForStrings(String timestampColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      stringsTableManipulation.setTimestampColumnName(timestampColumnNameForStrings);
   }

   public void setTimestampColumnTypeForStrings(String timestampColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      stringsTableManipulation.setTimestampColumnType(timestampColumnTypeForStrings);
   }

   public void setCreateTableOnStartForStrings(boolean createTableOnStartForStrings) {
      testImmutability("stringsTableManipulation");
      stringsTableManipulation.setCreateTableOnStart(createTableOnStartForStrings);
   }

   public void setDropTableOnExitForStrings(boolean dropTableOnExitForStrings) {
      testImmutability("stringsTableManipulation");
      stringsTableManipulation.setDropTableOnExit(dropTableOnExitForStrings);
   }

   public void setIdColumnNameForBinary(String idColumnNameForBinary) {
      binaryTableManipulation.setIdColumnName(idColumnNameForBinary);
   }

   public void setIdColumnTypeForBinary(String idColumnTypeForBinary) {
      testImmutability("stringsTableManipulation");
      binaryTableManipulation.setIdColumnType(idColumnTypeForBinary);
   }

   public void setTableNamePrefixForBinary(String tableNameForBinary) {
      testImmutability("binaryTableManipulation");
      if (tableNameForBinary == null) {
         throw new IllegalArgumentException("Null table name not allowed.");
      }
      if (tableNameForBinary.equals(stringsTableManipulation.getTableNamePrefix())) {
         throw new IllegalArgumentException("Same table name is used for both cache loaders, this is not allowed!");
      }
      binaryTableManipulation.setTableNamePrefix(tableNameForBinary);
   }

   public void setDataColumnNameForBinary(String dataColumnNameForBinary) {
      testImmutability("binaryTableManipulation");
      binaryTableManipulation.setDataColumnName(dataColumnNameForBinary);
   }

   public void setDataColumnTypeForBinary(String dataColumnTypeForBinary) {
      testImmutability("binaryTableManipulation");
      binaryTableManipulation.setDataColumnType(dataColumnTypeForBinary);
   }

   public void setTimestampColumnNameForBinary(String timestampColumnNameForBinary) {
      testImmutability("binaryTableManipulation");
      binaryTableManipulation.setTimestampColumnName(timestampColumnNameForBinary);
   }

   public void setTimestampColumnTypeForBinary(String timestampColumnTypeForBinary) {
      binaryTableManipulation.setTimestampColumnType(timestampColumnTypeForBinary);
   }

   public void setCreateTableOnStartForBinary(boolean createTableOnStartForBinary) {
      testImmutability("binaryTableManipulation");
      binaryTableManipulation.setCreateTableOnStart(createTableOnStartForBinary);
   }

   public void setDropTableOnExitForBinary(boolean dropTableOnExitForBinary) {
      testImmutability("binaryTableManipulation");
      binaryTableManipulation.setDropTableOnExit(dropTableOnExitForBinary);
   }

   public void setKey2StringMapperClass(String name) {
      testImmutability("key2StringMapper");
      key2StringMapper = name;
   }

   public void setLockConcurrencyLevelForStrings(int concurrencyLevel) {
      testImmutability("stringsConcurrencyLevel");
      stringsConcurrencyLevel = concurrencyLevel;
   }

   public void setLockConcurrencyLevelForBinary(int concurrencyLevel) {
      testImmutability("binaryConcurrencyLevel");
      binaryConcurrencyLevel = concurrencyLevel;
   }

   public void setLockAcquistionTimeout(int lockAcquistionTimeout) {
      testImmutability("lockAcquistionTimeout");
      this.lockAcquistionTimeout = lockAcquistionTimeout;
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getFetchSize()
    */
   public void setFetchSize(int fetchSize) {
      testImmutability("tableManipulation");
      binaryTableManipulation.setFetchSize(fetchSize);
      stringsTableManipulation.setFetchSize(fetchSize);
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getBatchSize()
    */
   public void setBatchSize(int batchSize) {
      testImmutability("tableManipulation");
      binaryTableManipulation.setBatchSize(batchSize);
      stringsTableManipulation.setBatchSize(batchSize);
   }

   public String getDatabaseType() {
      return binaryTableManipulation.databaseType == null ? "" : binaryTableManipulation.databaseType.toString();
   }

   /**
    * Sets the database dialect.  Valid types are reflected in the DatabaseType enum.  If unspecified, will attempt to
    * "guess" appropriate dialect from the JDBC driver specified.
    *
    * @param dbType
    */
   public void setDatabaseType(String dbType) {
      if (dbType != null) {
         DatabaseType type = DatabaseType.valueOf(dbType.toUpperCase().trim());
         binaryTableManipulation.databaseType = type;
         stringsTableManipulation.databaseType = type;
      }
   }

   @Override
   public JdbcMixedCacheStoreConfig clone() {
      JdbcMixedCacheStoreConfig dolly = (JdbcMixedCacheStoreConfig) super.clone();
      dolly.binaryTableManipulation = binaryTableManipulation.clone();
      dolly.stringsTableManipulation = stringsTableManipulation.clone();
      return dolly;
   }
}
