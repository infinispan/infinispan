package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStoreConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;

/**
 * Configureation for {@link org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcMixedCacheStoreConfig extends AbstractCacheStoreConfig {

   private ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();
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
      this.cacheLoaderClassName = JdbcMixedCacheStore.class.getName();
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
      if (key2StringMapper != null) config.setKey2StringMapperClass(key2StringMapper);
      return config;
   }

   public void setIdColumnNameForStrings(String idColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setIdColumnName(idColumnNameForStrings);
   }

   public void setIdColumnTypeForStrings(String idColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setIdColumnType(idColumnTypeForStrings);
   }

   public void setTableNamePrefixForStrings(String tableNameForStrings) {
      testImmutability("stringsTableManipulation");
      if (tableNameForStrings == null) throw new IllegalArgumentException("Null table name not allowed.");
      if (tableNameForStrings.equals(this.binaryTableManipulation.getTableNamePrefix())) {
         throw new IllegalArgumentException("Same table name is used for both cache loaders, this is not allowed!");
      }
      this.stringsTableManipulation.setTableNamePrefix(tableNameForStrings);
   }

   public void setDataColumnNameForStrings(String dataColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setDataColumnName(dataColumnNameForStrings);
   }

   public void setDataColumnTypeForStrings(String dataColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setDataColumnType(dataColumnTypeForStrings);
   }

   public void setTimestampColumnNameForStrings(String timestampColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setTimestampColumnName(timestampColumnNameForStrings);
   }

   public void setTimestampColumnTypeForStrings(String timestampColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setTimestampColumnType(timestampColumnTypeForStrings);
   }

   public void setCreateTableOnStartForStrings(boolean createTableOnStartForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setCreateTableOnStart(createTableOnStartForStrings);
   }

   public void setDropTableOnExitForStrings(boolean dropTableOnExitForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setDropTableOnExit(dropTableOnExitForStrings);
   }

   public void setIdColumnNameForBinary(String idColumnNameForBinary) {
      this.binaryTableManipulation.setIdColumnName(idColumnNameForBinary);
   }

   public void setIdColumnTypeForBinary(String idColumnTypeForBinary) {
      testImmutability("stringsTableManipulation");
      this.binaryTableManipulation.setIdColumnType(idColumnTypeForBinary);
   }

   public void setTableNamePrefixForBinary(String tableNameForBinary) {
      testImmutability("binaryTableManipulation");
      if (tableNameForBinary == null) throw new IllegalArgumentException("Null table name not allowed.");
      if (tableNameForBinary.equals(this.stringsTableManipulation.getTableNamePrefix())) {
         throw new IllegalArgumentException("Same table name is used for both cache loaders, this is not allowed!");
      }
      this.binaryTableManipulation.setTableNamePrefix(tableNameForBinary);
   }

   public void setDataColumnNameForBinary(String dataColumnNameForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setDataColumnName(dataColumnNameForBinary);
   }

   public void setDataColumnTypeForBinary(String dataColumnTypeForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setDataColumnType(dataColumnTypeForBinary);
   }

   public void setTimestampColumnNameForBinary(String timestampColumnNameForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setTimestampColumnName(timestampColumnNameForBinary);
   }
   
   public void setTimestampColumnTypeForBinary(String timestampColumnTypeForBinary) {
      this.binaryTableManipulation.setTimestampColumnType(timestampColumnTypeForBinary);
   }

   public void setCreateTableOnStartForBinary(boolean createTableOnStartForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setCreateTableOnStart(createTableOnStartForBinary);
   }
   
   public void setDropTableOnExitForBinary(boolean dropTableOnExitForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setDropTableOnExit(dropTableOnExitForBinary);
   }
   
   public void setDriverClass(String driverClass) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setDriverClass(driverClass);
   }

   public void setConnectionUrl(String connectionUrl) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionUrl(connectionUrl);
   }

   public void setUserName(String userName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setUserName(userName);
   }

   public void setPassword(String password) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setPassword(password);
   }

   /**
    * Name of the connection factory class.
    *
    * @see org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory
    */
   public void setConnectionFactoryClass(String connectionFactoryClass) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionFactoryClass(connectionFactoryClass);
   }

   public void setDatasourceJndiLocation(String location) {
      testImmutability("datasourceJndiLocation");
      this.connectionFactoryConfig.setDatasourceJndiLocation(location);
   }


   public ConnectionFactoryConfig getConnectionFactoryConfig() {
      return connectionFactoryConfig;
   }

   public void setKey2StringMapperClass(String name) {
      testImmutability("key2StringMapper");
      this.key2StringMapper = name;
   }

   public void setLockConcurrencyLevelForStrings(int concurrencyLevel) {
      testImmutability("stringsConcurrencyLevel");
      this.stringsConcurrencyLevel = concurrencyLevel;
   }

   public void setLockConcurrencyLevelForBinary(int concurrencyLevel) {
      testImmutability("binaryConcurrencyLevel");
      this.binaryConcurrencyLevel = concurrencyLevel;
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
      this.binaryTableManipulation.setFetchSize(fetchSize);
      this.stringsTableManipulation.setFetchSize(fetchSize);
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getBatchSize()
    */
   public void setBatchSize(int batchSize) {
      testImmutability("tableManipulation");
      this.binaryTableManipulation.setBatchSize(batchSize);
      this.stringsTableManipulation.setBatchSize(batchSize);
   }

   @Override
   public JdbcMixedCacheStoreConfig clone() {
      JdbcMixedCacheStoreConfig dolly = (JdbcMixedCacheStoreConfig) super.clone();
      dolly.connectionFactoryConfig = this.connectionFactoryConfig.clone();
      dolly.binaryTableManipulation = this.binaryTableManipulation.clone();
      dolly.stringsTableManipulation = this.stringsTableManipulation.clone();
      return dolly;
   }
}
