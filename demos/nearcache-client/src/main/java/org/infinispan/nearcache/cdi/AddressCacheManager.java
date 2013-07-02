package org.infinispan.nearcache.cdi;

import org.infinispan.Cache;

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheRemoveAll;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Global operations for the address cache
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Named @ApplicationScoped
public class AddressCacheManager {

   @Inject @AddressCache
   private Cache<CacheKey, Address> cache;

   public String[] getCachedValues() {
      List<String> values = new ArrayList<String>();
      for (Map.Entry<CacheKey, Address> entry : cache.entrySet())
         values.add(String.format("%s -> %s", entry.getKey(), entry.getValue()));

      return values.toArray(new String[values.size()]);
   }

   public int getNumberOfEntries() {
      return cache.size();
   }

   @CacheRemoveAll(cacheName = "address-cache")
   public void clearCache() {}

}
