package org.infinispan.nearcache.cdi;

import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemove;
import javax.cache.annotation.CacheResult;
import javax.cache.annotation.CacheValue;

/**
 * Address data access object
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class AddressDao {

   @CachePut(cacheName="address-cache")
   public String storeAddress(String personName, @CacheValue Address addr) {
      return String.format("%s lives in %s", personName, addr);
   }

   @CacheResult(cacheName="address-cache")
   public Address getAddress(String name) {
      return null; // No other source for addresses
   }

   @CacheRemove(cacheName="address-cache")
   public String removeAddress(String personName) {
      return String.format("%s no longer living there", personName);
   }

}
