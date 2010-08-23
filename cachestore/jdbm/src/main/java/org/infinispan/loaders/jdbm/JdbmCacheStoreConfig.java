package org.infinispan.loaders.jdbm;

import java.util.Comparator;

import org.infinispan.CacheException;
import org.infinispan.config.Dynamic;
import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.util.Util;

/**
 * Configures {@link JdbmCacheStore}.
 *
 * @author Elias Ross
 * @author Galder Zamarreño
 * @since 4.0
 */
public class JdbmCacheStoreConfig extends LockSupportCacheStoreConfig {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -3686035269816837880L;
   /**
    * @configRef desc="A location on disk where the store can write internal files"
    */
   String location = "jdbm";
   /**
    * @configRef desc="Comparator class used to sort the keys by the cache loader.
    * This should only need to be set when using keys that do not have a natural ordering."
    */
   String comparatorClassName = NaturalComparator.class.getName();

   /**
    * @configRef desc="Whenever a new entry is stored, an expiry entry is created and added
    * to the a queue that is later consumed by the eviction thread. This parameter sets the size
    * of this queue."
    */
   @Dynamic
   int expiryQueueSize = 10000;

   public JdbmCacheStoreConfig() {
      setCacheLoaderClassName(JdbmCacheStore.class.getName());
   }

   public String getLocation() {
      return location;
   }

   public void setLocation(String location) {
      testImmutability("location");
      this.location = location;
   }

   public String getComparatorClassName() {
      return comparatorClassName;
   }

   public void setComparatorClassName(String comparatorClassName) {
      testImmutability("comparatorClassName");
      this.comparatorClassName = comparatorClassName;
   }

   public int getExpiryQueueSize() {
      return expiryQueueSize;
   }

   public void setExpiryQueueSize(int expiryQueueSize) {
      testImmutability("expiryQueueSize");
      this.expiryQueueSize = expiryQueueSize;
   }

   /**
    * Returns a new comparator instance based on {@link #setComparatorClassName(String)}.
    */
   public Comparator createComparator() {
      return (Comparator) Util.getInstance(comparatorClassName);
   }

}
