package org.infinispan.loaders.bdbje;

import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.ConfigurationProperty;
import org.infinispan.config.ConfigurationElement.Cardinality;
import org.infinispan.loaders.AbstractCacheStoreConfig;

/**
 * Configures {@link org.infinispan.loaders.bdbje.BdbjeCacheStore}.  This allows you to tune a number of characteristics
 * of the {@link BdbjeCacheStore}.
 * <p/>
 * <ul> <li><tt>location</tt> - a location on disk where the store can write internal files.  This defaults to
 * <tt>Infinispan-BdbjeCacheStore</tt> in the current working directory.</li> <li><tt>lockAcquistionTimeout</tt> - the
 * length of time, in milliseconds, to wait for locks before timing out and throwing an exception.  By default, this is
 * set to <tt>60000</tt>.</li> <li><tt>maxTxRetries</tt> - the number of times transaction prepares will attempt to
 * resolve a deadlock before throwing an exception.  By default, this is set to <tt>5</tt>.</li>
 * <p/>
 * <li><tt>cacheDbName</tt> - the name of the SleepyCat database persisting this store.  This defaults to <tt>{@link
 * org.infinispan.Cache#getName()} cache#name}</tt>.</li> <li><tt>catalogDbName</tt> - the name of the SleepyCat
 * database persisting the class information for objects in this store.  This defaults to <tt>{@link
 * org.infinispan.Cache#getName()} cache#name}_class_catalog</tt>.</li>
 * <p/>
 * </ul>
 * <p/>
 * Please see {@link AbstractCacheStoreConfig} for more configuration parameters.
 *
 * @author Adrian Cole
 * @since 4.0
 */
@ConfigurationElements(elements = {
      @ConfigurationElement(name = "loader", parent = "loaders",
                            description = "org.infinispan.loaders.bdbje.BdbjeCacheStore",
                            cardinalityInParent = Cardinality.UNBOUNDED),
      @ConfigurationElement(name = "properties", parent = "loader")})
public class BdbjeCacheStoreConfig extends AbstractCacheStoreConfig {
   private String location = "Infinispan-BdbjeCacheStore";
   private long lockAcquistionTimeout = 60 * 1000;
   private int maxTxRetries = 5;
   private String cacheDbNamePrefix;
   private String catalogDbName;
   private String expiryDbPrefix;
   private String cacheName;

   public String getExpiryDbPrefix() {
      return expiryDbPrefix;
   }

   public String getExpiryDbName() {
      if (expiryDbPrefix != null) {
         return expiryDbPrefix + "_" + cacheName;
      } else {
         return cacheName + "_expiry";
      }
   }

   @ConfigurationProperty(name = "expiryDbNamePrefix", parentElement = "properties")
   public void setExpiryDbNamePrefix(String expiryDbName) {
      this.expiryDbPrefix = expiryDbName;
   }


   public BdbjeCacheStoreConfig() {
      setCacheLoaderClassName(BdbjeCacheStore.class.getName());
   }

   public int getMaxTxRetries() {
      return maxTxRetries;
   }


   @ConfigurationProperty(name = "maxTxRetries",
                          parentElement = "properties")
   public void setMaxTxRetries(int maxTxRetries) {
      this.maxTxRetries = maxTxRetries;
   }


   public long getLockAcquistionTimeout() {
      return lockAcquistionTimeout;
   }


   @ConfigurationProperty(name = "lockAcquistionTimeout",
                          parentElement = "properties")
   public void setLockAcquistionTimeout(long lockAcquistionTimeout) {
      this.lockAcquistionTimeout = lockAcquistionTimeout;
   }

   public String getLocation() {
      return location;
   }


   @ConfigurationProperty(name = "location",
                          parentElement = "properties")
   public void setLocation(String location) {
      testImmutability("location");
      this.location = location;
   }


   public String getCacheDbNamePrefix() {
      return cacheDbNamePrefix;
   }


   @ConfigurationProperty(name = "cacheDbNamePrefix",
                          parentElement = "properties")
   public void setCacheDbNamePrefix(String cacheDbNamePrefix) {
      this.cacheDbNamePrefix = cacheDbNamePrefix;
   }

   public String getCatalogDbName() {
      return catalogDbName;
   }


   @ConfigurationProperty(name = "catalogDbName",
                          parentElement = "properties")
   public void setCatalogDbName(String catalogDbName) {
      this.catalogDbName = catalogDbName;
   }

   void setCacheName(String name) {
      this.cacheName = name;
   }

   public String getCacheDbName() {
      if (cacheDbNamePrefix != null) {
         return cacheDbNamePrefix + "_" + cacheName;
      } else {
         return cacheName;
      }
   }
}
