package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.loaders.jdbc.AbstractNonDelegatingJdbcCacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.Key2StringMapper;
import org.infinispan.commons.util.Util;

/**
 * Configuration for {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore} cache store.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.keymappers.Key2StringMapper
 */
public class JdbcStringBasedCacheStoreConfig extends AbstractNonDelegatingJdbcCacheStoreConfig {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = 8835350707132331983L;

   private Key2StringMapper key2StringMapper;

   public JdbcStringBasedCacheStoreConfig(ConnectionFactoryConfig connectionFactoryConfig, TableManipulation tableManipulation) {
      super(connectionFactoryConfig, tableManipulation);
      this.cacheLoaderClassName = JdbcStringBasedCacheStore.class.getName();
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
            key2StringMapper = DefaultTwoWayKey2StringMapper.class.newInstance();
         } catch (Exception e) {
            throw new IllegalStateException("This should never happen", e);
         }
      }
      return key2StringMapper;
   }

   /**
    * Name of the class implementing Key2StringMapper. The default value is {@link org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper}
    *
    * @see org.infinispan.loaders.keymappers.Key2StringMapper
    */
   public void setKey2StringMapperClass(String className) {
      testImmutability("key2StringMapper");
      key2StringMapper = (Key2StringMapper) Util.getInstance(className, getClassLoader());
      setProperty(className, "key2StringMapperClass", getProperties());
   }

   /**
    * Sets the prefix for the name of the table where the data will be stored. "_<cache name>" will be appended
    * to this prefix in order to enforce unique table names for each cache.
    */
   public void setTableNamePrefix(String stringsTableNamePrefix) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTableNamePrefix(stringsTableNamePrefix);
   }

   public void setStringsTableNamePrefix(String stringsTableNamePrefix) {
      setTableNamePrefix(stringsTableNamePrefix);
   }

   @Override
   public JdbcStringBasedCacheStoreConfig clone() {
      JdbcStringBasedCacheStoreConfig result = (JdbcStringBasedCacheStoreConfig) super.clone();
      result.key2StringMapper = key2StringMapper;
      return result;
   }

   @Override
   public String toString() {
      return "JdbcStringBasedCacheStoreConfig{" +
            "key2StringMapper=" + key2StringMapper +
            "} " + super.toString();
   }
}
