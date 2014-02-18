package org.infinispan.persistence;

import org.infinispan.persistence.spi.AdvancedCacheLoader;

import java.util.Collection;

/**
 * Filter based on accepting/rejecting the keys that are present in a supplied collection.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class CollectionKeyFilter implements AdvancedCacheLoader.KeyFilter<Object> {

   Collection acceptedKeys = null;
   private boolean accept = false;

   public CollectionKeyFilter(Collection rejectedKeys) {
      this.acceptedKeys = rejectedKeys;
   }

   public CollectionKeyFilter(Collection rejectedKeys, boolean accept) {
      this.acceptedKeys = rejectedKeys;
      this.accept = accept;
   }


   @Override
   public boolean shouldLoadKey(Object key) {
      return accept ? acceptedKeys.contains(key) : !acceptedKeys.contains(key);
   }
}
