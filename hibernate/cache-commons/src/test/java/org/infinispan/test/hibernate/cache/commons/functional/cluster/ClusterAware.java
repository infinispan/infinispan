package org.infinispan.test.hibernate.cache.commons.functional.cluster;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Hashtable;

import org.infinispan.manager.EmbeddedCacheManager;

public class ClusterAware {
   public static final Hashtable<String, EmbeddedCacheManager> cacheManagers = new Hashtable<String, EmbeddedCacheManager>();

   public static EmbeddedCacheManager getCacheManager(String name) {
      return cacheManagers.get(name);
   }

   public static void addCacheManager(String name, EmbeddedCacheManager manager) {
      assertNull(cacheManagers.put(name, manager));
   }

   public static void removeCacheManager(String name) {
      assertNotNull(cacheManagers.remove(name));
   }
}
