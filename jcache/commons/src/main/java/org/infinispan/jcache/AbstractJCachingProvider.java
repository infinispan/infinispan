package org.infinispan.jcache;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import org.infinispan.jcache.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Shared behavior for the embedded and client-server implementations.
 * 
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 */
public abstract class AbstractJCachingProvider implements CachingProvider {

   private static final Log log = LogFactory.getLog(AbstractJCachingProvider.class, Log.class);

   /**
    * Keeps track of cache managers. Each cache manager has to be tracked
    * based on its name and class loader. So, you could be have cache managers
    * registered with the same name but different class loaders, resulting in
    * different cache manager instances.
    *
    * A solution based around weak value references to cache managers won't
    * work here, because if the user does not have any references to the
    * cache managers, these would disappear from the map. Users are not
    * required to keep strong references to cache managers. They can simply
    * get cache manager references via
    * {@link javax.cache.spi.CachingProvider#getCacheManager()}.
    *
    * So, the only possible way to avoid leaking cache managers is to have a
    * weak key hash map keyed on class loader. So when no other hard
    * references to the class loader are kept, the cache manager can be
    * garbage collected and its {@link #finalize()} method can be called
    * if the user forgot to shut down the cache manager.
    */
   private final Map<ClassLoader, Map<URI, CacheManager>> cacheManagers =
         new WeakHashMap<ClassLoader, Map<URI, CacheManager>>();

   @Override
   public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
      URI globalUri = uri == null ? getDefaultURI() : uri;
      ClassLoader globalClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
      Properties globalProperties = properties == null ? new Properties() : properties;

      synchronized (cacheManagers) {
         Map<URI, CacheManager> map = cacheManagers.get(globalClassLoader);
         if (map == null) {
            if (log.isTraceEnabled())
               log.tracef("No cache managers registered under '%s'", globalUri);

            map = new HashMap<URI, CacheManager>();
            cacheManagers.put(globalClassLoader, map);
         }

         CacheManager cacheManager = map.get(globalUri);
         if (cacheManager == null || cacheManager.isClosed()) {
            // Not found or stopped, create cache manager and add to collection
            cacheManager = createCacheManager(globalClassLoader, globalUri, globalProperties);
            if (log.isTraceEnabled())
               log.tracef("Created '%s' cache manager", globalUri);

            map.put(globalUri, cacheManager);
         }

         return cacheManager;
      }
   }

   @Override
   public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
      return getCacheManager(uri, classLoader, getDefaultProperties());
   }

   @Override
   public ClassLoader getDefaultClassLoader() {
      return getClass().getClassLoader();
   }

   @Override
   public Properties getDefaultProperties() {
      return null;
   }

   @Override
   public CacheManager getCacheManager() {
      return getCacheManager(getDefaultURI(), getDefaultClassLoader());
   }

   @Override
   public void close() {
      synchronized (cacheManagers) {
         for (Map<URI, CacheManager> map : cacheManagers.values())
            close(map);

         cacheManagers.clear();
         if (log.isTraceEnabled())
            log.tracef("All cache managers have been removed");
      }
   }

   @Override
   public void close(ClassLoader classLoader) {
      close(null, classLoader);
   }

   @Override
   public void close(URI uri, ClassLoader classLoader) {
      synchronized (cacheManagers) {
         if (uri != null) {
            Map<URI, CacheManager> map = cacheManagers.get(classLoader);
            if (map != null) {
               CacheManager cacheManager = map.remove(uri);
               if (map.isEmpty())
                  cacheManagers.remove(classLoader);

               if (cacheManager != null)
                  cacheManager.close();
            }
         } else {
            Map<URI, CacheManager> cacheManagersToClose = cacheManagers.remove(classLoader);
            if (cacheManagersToClose != null)
               close(cacheManagersToClose);
         }
      }
   }

   private void close(Map<URI, CacheManager> map) {
      for (CacheManager cacheManager : map.values()) {
         cacheManager.close();
         if (log.isTraceEnabled())
            log.tracef("Shutdown cache manager '%s'", cacheManager.getURI());
      }
   }

   protected abstract CacheManager createCacheManager(
         ClassLoader classLoader, URI uri, Properties properties);

}
