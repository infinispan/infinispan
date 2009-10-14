package org.infinispan.loaders.jdbc.binary;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;

/**
 * Defines available configuration elements for {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcBinaryCacheStoreConfig extends LockSupportCacheStoreConfig {

   private ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();
   private TableManipulation tableManipulation = new TableManipulation();
   private boolean createConnectionFatory = true;

   public JdbcBinaryCacheStoreConfig(boolean createConnectionFatory) {
      this.createConnectionFatory = createConnectionFatory;
   }

   public JdbcBinaryCacheStoreConfig(ConnectionFactoryConfig connectionFactoryConfig, TableManipulation tm) {
      this();
      this.connectionFactoryConfig = connectionFactoryConfig;
      this.tableManipulation = tm;
   }

   public JdbcBinaryCacheStoreConfig() {
      cacheLoaderClassName = JdbcBinaryCacheStore.class.getName();
   }

   boolean isManageConnectionFatory() {
      return createConnectionFatory;
   }

   /**
    * If true, and the table is missing it will be created when starting the cache store. Default to <tt>true</tt>.
    */
   public void setCreateTableOnStart(boolean createTableOnStart) {
      testImmutability("tableManipulation");
      tableManipulation.setCreateTableOnStart(createTableOnStart);
   }

   /**
    * If true, the table will be created when cache store is stopped. Default to <tt>false</tt>.
    */
   public void setDropTableOnExit(boolean dropTableOnExit) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDropTableOnExit(dropTableOnExit);
   }

   /**
    * Sets the prefix for the name of the table where the data will be stored. "_<cache name>" will be appended
    * to this prefix in order to enforce unique table names for each cache.
    */
   public void setBucketTableNamePrefix(String bucketTableName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTableNamePrefix(bucketTableName);
   }

   public void setIdColumnName(String idColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setIdColumnName(idColumnName);
   }

   public void setIdColumnType(String idColumnType) {
      testImmutability("tableManipulation");
      this.tableManipulation.setIdColumnType(idColumnType);
   }

   public void setDataColumnName(String dataColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDataColumnName(dataColumnName);
   }

   public void setDataColumnType(String dataColumnType) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDataColumnType(dataColumnType);
   }

   public void setTimestampColumnName(String timestampColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTimestampColumnName(timestampColumnName);
   }


   public void setTimestampColumnType(String timestampColumnType) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTimestampColumnType(timestampColumnType);
   }

   /**
    * Url connection to the database.
    */
   
   public void setConnectionUrl(String connectionUrl) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionUrl(connectionUrl);
   }

   /**
    * Databse user name.
    */
   public void setUserName(String userName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setUserName(userName);
   }

   /**
    * Database username's password.
    */
   public void setPassword(String password) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setPassword(password);
   }


   public void setDatasourceJndiLocation(String location) {
      testImmutability("datasourceJndiLocation");
      this.connectionFactoryConfig.setDatasourceJndiLocation(location);
   }

   /**
    * Driver class, will be loaded before initializing the {@link org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory}
    */
   public void setDriverClass(String driverClass) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setDriverClass(driverClass);
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


   @Override
   public JdbcBinaryCacheStoreConfig clone() {
      JdbcBinaryCacheStoreConfig result = (JdbcBinaryCacheStoreConfig) super.clone();
      result.connectionFactoryConfig = connectionFactoryConfig.clone();
      result.tableManipulation = tableManipulation.clone();
      return result;
   }

   public ConnectionFactoryConfig getConnectionFactoryConfig() {
      return connectionFactoryConfig;
   }

   public TableManipulation getTableManipulation() {
      return tableManipulation;
   }

   public void setTableManipulation(TableManipulation tableManipulation) {
      testImmutability("tableManipulation");
      this.tableManipulation = tableManipulation;
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getFetchSize()
    */
   public void setFetchSize(int fetchSize) {
      testImmutability("tableManipulation");
      this.tableManipulation.setFetchSize(fetchSize);
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getBatchSize()
    */
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
