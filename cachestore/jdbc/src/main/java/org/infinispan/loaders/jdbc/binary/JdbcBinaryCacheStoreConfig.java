package org.infinispan.loaders.jdbc.binary;

import org.infinispan.loaders.jdbc.AbstractNonDelegatingJdbcCacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;

/**
 * Defines available configuration elements for {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcBinaryCacheStoreConfig extends AbstractNonDelegatingJdbcCacheStoreConfig {

   /** The serialVersionUID */
   private static final long serialVersionUID = 7659899424935453635L;

   public JdbcBinaryCacheStoreConfig(boolean manageConnectionFactory) {
      this.manageConnectionFactory = manageConnectionFactory;
   }

   public JdbcBinaryCacheStoreConfig(ConnectionFactoryConfig connectionFactoryConfig, TableManipulation tm) {
      super(connectionFactoryConfig, tm);
      this.cacheLoaderClassName = JdbcBinaryCacheStore.class.getName();
      this.connectionFactoryConfig = connectionFactoryConfig;
      this.tableManipulation = tm;
   }

   public JdbcBinaryCacheStoreConfig() {
      cacheLoaderClassName = JdbcBinaryCacheStore.class.getName();
   }

   /**
    * Sets the prefix for the name of the table where the data will be stored. "_<cache name>" will be appended
    * to this prefix in order to enforce unique table names for each cache.
    */
   public void setBucketTableNamePrefix(String bucketTableName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTableNamePrefix(bucketTableName);
   }

   public void setTableNamePrefix(String tableNamePrefix) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTableNamePrefix(tableNamePrefix);
   }

}
