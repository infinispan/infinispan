package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.ConfigurationProperty;
import org.infinispan.config.ConfigurationElement.Cardinality;
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
@ConfigurationElements(elements = {
         @ConfigurationElement(name = "loader", parent = "loaders", 
                  description = "org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStoreConfig",
                  cardinalityInParent=Cardinality.UNBOUNDED),
         @ConfigurationElement(name = "properties", parent = "loader") })
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
   
   @ConfigurationProperty(name="idColumnNameForStrings",
            parentElement="properties")
   public void setIdColumnNameForStrings(String idColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setIdColumnName(idColumnNameForStrings);
   }

   @ConfigurationProperty(name="idColumnTypeForStrings",
            parentElement="properties")
   public void setIdColumnTypeForStrings(String idColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setIdColumnType(idColumnTypeForStrings);
   }

   @ConfigurationProperty(name="tableNameForStrings",
            parentElement="properties")
   public void setTableNameForStrings(String tableNameForStrings) {
      testImmutability("stringsTableManipulation");
      if (tableNameForStrings == null) throw new IllegalArgumentException("Null table name not allowed.");
      if (tableNameForStrings.equals(this.binaryTableManipulation.getTableName())) {
         throw new IllegalArgumentException("Same table name is used for both cache loaders, this is not allowed!");
      }
      this.stringsTableManipulation.setTableName(tableNameForStrings);
   }

   @ConfigurationProperty(name="dataColumnNameForStrings",
            parentElement="properties")
   public void setDataColumnNameForStrings(String dataColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setDataColumnName(dataColumnNameForStrings);
   }

   @ConfigurationProperty(name="dataColumnTypeForStrings",
            parentElement="properties")
   public void setDataColumnTypeForStrings(String dataColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setDataColumnType(dataColumnTypeForStrings);
   }

   @ConfigurationProperty(name="timestampColumnNameForStrings",
            parentElement="properties")
   public void setTimestampColumnNameForStrings(String timestampColumnNameForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setTimestampColumnName(timestampColumnNameForStrings);
   }

   @ConfigurationProperty(name="timestampColumnTypeForStrings",
            parentElement="properties")
   public void setTimestampColumnTypeForStrings(String timestampColumnTypeForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setTimestampColumnType(timestampColumnTypeForStrings);
   }

   @ConfigurationProperty(name="createTableOnStartForStrings",
            parentElement="properties")
   public void setCreateTableOnStartForStrings(boolean createTableOnStartForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setCreateTableOnStart(createTableOnStartForStrings);
   }

   @ConfigurationProperty(name="dropTableOnExitForStrings",
            parentElement="properties")
   public void setDropTableOnExitForStrings(boolean dropTableOnExitForStrings) {
      testImmutability("stringsTableManipulation");
      this.stringsTableManipulation.setDropTableOnExit(dropTableOnExitForStrings);
   }

   @ConfigurationProperty(name="idColumnNameForBinary",
            parentElement="properties")
   public void setIdColumnNameForBinary(String idColumnNameForBinary) {
      this.binaryTableManipulation.setIdColumnName(idColumnNameForBinary);
   }

   @ConfigurationProperty(name="idColumnTypeForBinary",
            parentElement="properties")
   public void setIdColumnTypeForBinary(String idColumnTypeForBinary) {
      testImmutability("stringsTableManipulation");
      this.binaryTableManipulation.setIdColumnType(idColumnTypeForBinary);
   }

   @ConfigurationProperty(name="tableNameForBinary",
            parentElement="properties")
   public void setTableNameForBinary(String tableNameForBinary) {
      testImmutability("binaryTableManipulation");
      if (tableNameForBinary == null) throw new IllegalArgumentException("Null table name not allowed.");
      if (tableNameForBinary.equals(this.stringsTableManipulation.getTableName())) {
         throw new IllegalArgumentException("Same table name is used for both cache loaders, this is not allowed!");
      }
      this.binaryTableManipulation.setTableName(tableNameForBinary);
   }

   @ConfigurationProperty(name="dataColumnNameForBinary",
            parentElement="properties")
   public void setDataColumnNameForBinary(String dataColumnNameForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setDataColumnName(dataColumnNameForBinary);
   }

   @ConfigurationProperty(name="dataColumnTypeForBinary",
            parentElement="properties")
   public void setDataColumnTypeForBinary(String dataColumnTypeForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setDataColumnType(dataColumnTypeForBinary);
   }

   @ConfigurationProperty(name="timestampColumnNameForBinary",
            parentElement="properties")
   public void setTimestampColumnNameForBinary(String timestampColumnNameForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setTimestampColumnName(timestampColumnNameForBinary);
   }

   @ConfigurationProperty(name="timestampColumnTypeForBinary",
            parentElement="properties")
   public void setTimestampColumnTypeForBinary(String timestampColumnTypeForBinary) {
      this.binaryTableManipulation.setTimestampColumnType(timestampColumnTypeForBinary);
   }

   @ConfigurationProperty(name="createTableOnStartForBinary",
            parentElement="properties")
   public void setCreateTableOnStartForBinary(boolean createTableOnStartForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setCreateTableOnStart(createTableOnStartForBinary);
   }

   @ConfigurationProperty(name="dropTableOnExitForBinary",
            parentElement="properties")
   public void setDropTableOnExitForBinary(boolean dropTableOnExitForBinary) {
      testImmutability("binaryTableManipulation");
      this.binaryTableManipulation.setDropTableOnExit(dropTableOnExitForBinary);
   }

   @ConfigurationProperty(name="driverClass",
            parentElement="properties")
   public void setDriverClass(String driverClass) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setDriverClass(driverClass);
   }

   @ConfigurationProperty(name="connectionUrl",
            parentElement="properties")
   public void setConnectionUrl(String connectionUrl) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionUrl(connectionUrl);
   }

   @ConfigurationProperty(name="userName",
            parentElement="properties")
   public void setUserName(String userName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setUserName(userName);
   }

   @ConfigurationProperty(name="password",
            parentElement="properties")
   public void setPassword(String password) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setPassword(password);
   }

   public ConnectionFactoryConfig getConnectionFactoryConfig() {
      return connectionFactoryConfig;
   }

   @ConfigurationProperty(name="key2StringMapperClass",
            parentElement="properties")
   public void setKey2StringMapperClass(String name) {
      testImmutability("key2StringMapper");
      this.key2StringMapper = name;
   }

   @ConfigurationProperty(name="lockConcurrencyLevelForStrings",
            parentElement="properties")
   public void setLockConcurrencyLevelForStrings(int concurrencyLevel) {
      testImmutability("stringsConcurrencyLevel");
      this.stringsConcurrencyLevel = concurrencyLevel;
   }

   @ConfigurationProperty(name="lockConcurrencyLevelForBinary",
            parentElement="properties")
   public void setLockConcurrencyLevelForBinary(int concurrencyLevel) {
      testImmutability("binaryConcurrencyLevel");
      this.binaryConcurrencyLevel = concurrencyLevel;
   }

   @ConfigurationProperty(name="lockAcquistionTimeout",
            parentElement="properties")
   public void setLockAcquistionTimeout(int lockAcquistionTimeout) {
      testImmutability("lockAcquistionTimeout");
      this.lockAcquistionTimeout = lockAcquistionTimeout;
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getFetchSize()
    */
   @ConfigurationProperty(name="fetchSize",
            parentElement="properties")
   public void setFetchSize(int fetchSize) {
      testImmutability("tableManipulation");
      this.binaryTableManipulation.setFetchSize(fetchSize);
      this.stringsTableManipulation.setFetchSize(fetchSize);
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getBatchSize()
    */
   @ConfigurationProperty(name="batchSize",
            parentElement="properties")
   public void setBatchSize(int batchSize) {
      testImmutability("tableManipulation");
      this.binaryTableManipulation.setBatchSize(batchSize);
      this.stringsTableManipulation.setBatchSize(batchSize);
   }
}
