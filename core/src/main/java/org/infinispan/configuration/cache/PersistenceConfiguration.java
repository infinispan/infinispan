package org.infinispan.configuration.cache;

import java.util.List;

/**
 * Configuration for stores.
 *
 */
public class PersistenceConfiguration {

   private final boolean passivation;
   private final List<StoreConfiguration> stores;

   PersistenceConfiguration(boolean passivation, List<StoreConfiguration> stores) {
      this.passivation = passivation;
      this.stores = stores;
   }

   /**
    * If true, data is only written to the cache store when it is evicted from memory, a phenomenon
    * known as 'passivation'. Next time the data is requested, it will be 'activated' which means
    * that data will be brought back to memory and removed from the persistent store. This gives you
    * the ability to 'overflow' to disk, similar to swapping in an operating system. <br />
    * <br />
    * If false, the cache store contains a copy of the contents in memory, so writes to cache result
    * in cache store writes. This essentially gives you a 'write-through' configuration.
    */
   public boolean passivation() {
      return passivation;
   }

   public List<StoreConfiguration> stores() {
      return stores;
   }

   /**
    * Loops through all individual cache loader configs and checks if fetchPersistentState is set on
    * any of them
    */
   public Boolean fetchPersistentState() {
      for (StoreConfiguration c : stores) {
         if (c.fetchPersistentState())
            return true;
      }
      return false;
   }

   /**
    * Loops through all individual cache loader configs and checks if preload is set on
    * any of them
    */
   public Boolean preload() {
      for (StoreConfiguration c : stores) {
         if (c.preload())
            return true;
      }
      return false;
   }

   public boolean usingStores() {
      return !stores.isEmpty();
   }

   public boolean usingAsyncStore() {
      for (StoreConfiguration c : stores) {
         if (c.async().enabled())
            return true;
      }
      return false;
   }

   @Override
   public String toString() {
      return "PersistenceConfiguration{" +
            "persistence=" + stores +
            ", passivation=" + passivation +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PersistenceConfiguration that = (PersistenceConfiguration) o;

      if (passivation != that.passivation) return false;
      if (stores != null ? !stores.equals(that.stores) : that.stores != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (passivation ? 1 : 0);
      result = 31 * result + (stores != null ? stores.hashCode() : 0);
      return result;
   }

}
