package org.infinispan.loaders.bdbje;

import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
 * <li><tt>environmentPropertiesFile</tt> - the name of the SleepyCat properties file containing <tt>je.*</tt>
 * properties to initialize the JE environment.  Defaults to null, no properties are passed in to the JE engine if this
 * is null or empty.  The file specified needs to be available on the classpath, or must be an absolute path to a valid
 * properties file.  Refer to SleepyCat JE Environment configuration documentation for details.</tt>.</li>
 * <p/>
 * </ul>
 * <p/>
 * Please see {@link AbstractCacheStoreConfig} for more configuration parameters.
 *
 * @author Adrian Cole
 * @since 4.0
 */
public class BdbjeCacheStoreConfig extends AbstractCacheStoreConfig {
   private String location = "Infinispan-BdbjeCacheStore";
   private long lockAcquistionTimeout = 60 * 1000;
   private int maxTxRetries = 5;
   private String cacheDbNamePrefix;
   private String catalogDbName;
   private String expiryDbPrefix;
   private String cacheName;
   private String environmentPropertiesFile;
   private static final long serialVersionUID = -2913308899139287416L;

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

   public void setExpiryDbNamePrefix(String expiryDbName) {
      this.expiryDbPrefix = expiryDbName;
   }


   public BdbjeCacheStoreConfig() {
      setCacheLoaderClassName(BdbjeCacheStore.class.getName());
   }

   public int getMaxTxRetries() {
      return maxTxRetries;
   }

   public void setMaxTxRetries(int maxTxRetries) {
      this.maxTxRetries = maxTxRetries;
   }


   public long getLockAcquistionTimeout() {
      return lockAcquistionTimeout;
   }

   public void setLockAcquistionTimeout(long lockAcquistionTimeout) {
      this.lockAcquistionTimeout = lockAcquistionTimeout;
   }

   public String getLocation() {
      return location;
   }

   public void setLocation(String location) {
      testImmutability("location");
      this.location = location;
   }


   public String getCacheDbNamePrefix() {
      return cacheDbNamePrefix;
   }

   public void setCacheDbNamePrefix(String cacheDbNamePrefix) {
      this.cacheDbNamePrefix = cacheDbNamePrefix;
   }

   public String getCatalogDbName() {
      return catalogDbName;
   }

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

   public String getEnvironmentPropertiesFile() {
      return environmentPropertiesFile;
   }

   public void setEnvironmentPropertiesFile(String environmentPropertiesFile) {
      this.environmentPropertiesFile = environmentPropertiesFile;
   }

   public Properties readEnvironmentProperties() throws CacheLoaderException {
      if (environmentPropertiesFile == null || environmentPropertiesFile.trim().length() == 0) return null;
      InputStream i = FileLookupFactory.newInstance().lookupFile(environmentPropertiesFile, getClassLoader());
      if (i != null) {
         Properties p = new Properties();
         try {
            p.load(i);
         } catch (IOException ioe) {
            throw new CacheLoaderException("Unable to read environment properties file " + environmentPropertiesFile, ioe);
         } finally {
            Util.close(i);
         }
         return p;
      } else {
         return null;
      }
   }
}
