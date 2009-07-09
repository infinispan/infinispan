package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.ConfigurationProperty;
import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.util.Util;

/**
 * Configuration for {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore} cache store.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.jdbc.stringbased.Key2StringMapper
 */
@ConfigurationElements(elements = {
         @ConfigurationElement(name = "loader", parent = "loaders", 
                  description = "org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig"),
         @ConfigurationElement(name = "properties", parent = "loader") })
public class JdbcStringBasedCacheStoreConfig extends LockSupportCacheStoreConfig {

   private Key2StringMapper key2StringMapper;

   private ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();
   private TableManipulation tableManipulation = new TableManipulation();
   private boolean manageConnectionFactory = true;

   public JdbcStringBasedCacheStoreConfig(ConnectionFactoryConfig connectionFactoryConfig, TableManipulation tableManipulation) {
      this();
      this.connectionFactoryConfig = connectionFactoryConfig;
      this.tableManipulation = tableManipulation;
   }

   public JdbcStringBasedCacheStoreConfig() {
      cacheLoaderClassName = JdbcStringBasedCacheStore.class.getName();
   }

   public JdbcStringBasedCacheStoreConfig(boolean manageConnectionFactory) {
      this();
      this.manageConnectionFactory = manageConnectionFactory;
   }

   public Key2StringMapper getKey2StringMapper() {
      if (key2StringMapper == null) {
         try {
            key2StringMapper = DefaultKey2StringMapper.class.newInstance();
         } catch (Exception e) {
            throw new IllegalStateException("This should never happen", e);
         }
      }
      return key2StringMapper;
   }

   /**
    * Name of the class implementing Key2StringMapper. The default value is {@link org.infinispan.loaders.jdbc.stringbased.DefaultKey2StringMapper}
    *
    * @see org.infinispan.loaders.jdbc.stringbased.Key2StringMapper
    */
   @ConfigurationProperty(name="key2StringMapper",
            parentElement="properties")
   public void setKey2StringMapperClass(String className) {
      testImmutability("key2StringMapper");
      try {
         key2StringMapper = (Key2StringMapper) Util.getInstance(className);
      } catch (Exception e) {
         throw new IllegalArgumentException("Could not load Key2StringMapper :'" + className + "'", e);
      }
   }

   /**
    * Sets the name of the table where data will be stored.
    */
   @ConfigurationProperty(name="stringsTableName",
            parentElement="properties")
   public void setStringsTableName(String stringsTableName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTableName(stringsTableName);
   }

   /**
    * Sets the name of the column where the id will be stored. The id is obtained through:
    * <pre>
    *   key2StringMapper.getStringMapping(storedEntry.getKey());
    * </pre>
    * Mandatory.
    */
   @ConfigurationProperty(name="idColumnName",
            parentElement="properties")
   public void setIdColumnName(String idColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setIdColumnName(idColumnName);
   }

   /**
    * Sets the name of the column where the StoredEntry will be binary stored. Mandatory.
    */
   @ConfigurationProperty(name="dataColumnName",
            parentElement="properties")
   public void setDataColumnName(String dataColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDataColumnName(dataColumnName);
   }

   /**
    * Sets the name of the column where the timestamp (Long in java) will be stored. Mandatory.
    */
   @ConfigurationProperty(name="timestampColumnName",
            parentElement="properties")
   public void setTimestampColumnName(String timestampColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTimestampColumnName(timestampColumnName);
   }

   @ConfigurationProperty(name="connectionFactoryClass",
            parentElement="properties")
   public void setConnectionFactoryClass(String connectionFactoryClass) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionFactoryClass(connectionFactoryClass);
   }

   public ConnectionFactoryConfig getConnectionFactoryConfig() {
      return connectionFactoryConfig;
   }

   public TableManipulation getTableManipulation() {
      return tableManipulation;
   }

   /**
    * Jdbc connection string for connecting to the database. Mandatory.
    */
   @ConfigurationProperty(name="connectionUrl",
            parentElement="properties")
   public void setConnectionUrl(String connectionUrl) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionUrl(connectionUrl);
   }

   /**
    * Database username.
    */
   @ConfigurationProperty(name="userName",
            parentElement="properties")
   public void setUserName(String userName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setUserName(userName);
   }

   /**
    * Database username's password.
    */
   @ConfigurationProperty(name="password",
            parentElement="properties")
   public void setPassword(String password) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setPassword(password);
   }

   /**
    * The name of the driver used for connecting to the database. Mandatory, will be loaded before initiating the first
    * connection.
    */
   @ConfigurationProperty(name="driverClass",
            parentElement="properties")
   public void setDriverClass(String driverClassName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setDriverClass(driverClassName);
   }

   /**
    * sql equivalent for java's String. Mandatory.
    */
   @ConfigurationProperty(name="idColumnType",
            parentElement="properties")
   public void setIdColumnType(String idColumnType) {
      testImmutability("tableManipulation");
      this.tableManipulation.setIdColumnType(idColumnType);
   }

   /**
    * Sets the type of the column where data will be binary stored. BLOB-like type, DBMS dependent. Mandatory.
    */
   @ConfigurationProperty(name="dataColumnType",
            parentElement="properties")
   public void setDataColumnType(String dataColumnType) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDataColumnType(dataColumnType);
   }

   @ConfigurationProperty(name="dropTableOnExit",
            parentElement="properties")
   public void setDropTableOnExit(boolean dropTableOnExit) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDropTableOnExit(dropTableOnExit);
   }


   @ConfigurationProperty(name="createTableOnStart",
            parentElement="properties")
   public void setCreateTableOnStart(boolean createTableOnStart) {
      testImmutability("tableManipulation");
      this.tableManipulation.setCreateTableOnStart(createTableOnStart);
   }

   /**
    * If this method returns false, then the connection factory should not be created by the {@link
    * org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore}, but will be injected through {@link
    * org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore#doConnectionFactoryInitialization(org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory)}
    */
   boolean isManageConnectionFactory() {
      return manageConnectionFactory;
   }

   public void setTableManipulation(TableManipulation tableManipulation) {
      testImmutability("tableManipulation");
      this.tableManipulation = tableManipulation;
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getFetchSize()
    */
   @ConfigurationProperty(name="fetchSize",
            parentElement="properties")
   public void setFetchSize(int fetchSize) {
      testImmutability("tableManipulation");
      this.tableManipulation.setFetchSize(fetchSize);
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getBatchSize()
    */
   @ConfigurationProperty(name="batchSize",
            parentElement="properties")
   public void setBatchSize(int batchSize) {
      testImmutability("tableManipulation");
      this.tableManipulation.setBatchSize(batchSize);
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getFetchSize()
    */
   public int getFetchSize() {
      return this.tableManipulation.getFetchSize();
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getBatchSize()
    */
   public int getBatchSize() {
      return this.tableManipulation.getBatchSize();
   }
}
