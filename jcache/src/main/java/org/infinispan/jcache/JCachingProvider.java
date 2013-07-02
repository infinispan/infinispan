package org.infinispan.jcache;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.CacheManager;
import javax.cache.CachingShutdownException;
import javax.cache.OptionalFeature;
import javax.cache.Status;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

/**
 * Infinispan's SPI hook up to {@link javax.cache.spi.CachingProvider}.
 * 
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCachingProvider implements CachingProvider {

   private static final Log log = LogFactory.getLog(JCachingProvider.class);

   private static final URI DEFAULT_URI = URI.create(JCachingProvider.class.getName());

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
   private final Map<ClassLoader, Map<URI, JCacheManager>> cacheManagers =
         new WeakHashMap<ClassLoader, Map<URI, JCacheManager>>();

   @Override
   public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
      return getCacheManager(uri, classLoader);
   }

   @Override
   public CacheManager getCacheManager(URI managerUri, ClassLoader managerClassLoader) {
      URI uri = managerUri == null ? getDefaultURI() : managerUri;
      ClassLoader classLoader = managerClassLoader == null ? getDefaultClassLoader() : managerClassLoader;

      synchronized (cacheManagers) {
         Map<URI, JCacheManager> map = cacheManagers.get(classLoader);
         if (map == null) {
            if (log.isTraceEnabled())
               log.tracef("No cache managers registered under '%s'", uri);

            map = new HashMap<URI, JCacheManager>();
            cacheManagers.put(classLoader, map);
         }

         JCacheManager cacheManager= map.get(uri);
         if (cacheManager == null || cacheManager.getStatus() == Status.STOPPED) {
            // Not found or stopped, create cache manager and add to collection
            cacheManager = createCacheManager(classLoader, uri);
            if (log.isTraceEnabled())
               log.tracef("Created '%s' cache manager", uri);

            map.put(uri, cacheManager);
         }

         return cacheManager;
      }
   }

   @Override
   public ClassLoader getDefaultClassLoader() {
      return getClass().getClassLoader();
   }

   @Override
   public URI getDefaultURI() {
      return DEFAULT_URI;
   }

   @Override
   public Properties getDefaultProperties() {
      return null;
   }

   @Override
   public CacheManager getCacheManager() {
      return getCacheManager(DEFAULT_URI, getDefaultClassLoader());
   }

   @Override
   public boolean isSupported(OptionalFeature optionalFeature) {
      switch (optionalFeature) {
         case TRANSACTIONS:
            return true;
         case STORE_BY_REFERENCE:
            return true;
         default:
            return false;
      }
   }

   @Override
   public void close() {
      synchronized (cacheManagers) {
         Map<CacheManager, Exception> failures =
               new IdentityHashMap<CacheManager, Exception>();
         for (Map<URI, JCacheManager> map : cacheManagers.values()) {
            try {
               close(map);
            } catch (CachingShutdownException e) {
               failures.putAll(e.getFailures());
            }
         }
         cacheManagers.clear();
         if (log.isTraceEnabled())
            log.tracef("All cache managers have been removed");

         if (!failures.isEmpty()) {
            throw new CachingShutdownException(failures);
         }
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
            Map<URI, JCacheManager> map = cacheManagers.get(classLoader);
            JCacheManager cacheManager = map.remove(uri);
            if (map.isEmpty())
               cacheManagers.remove(classLoader);

            if (cacheManager != null)
               cacheManager.close();
         } else {
            Map<URI, JCacheManager> cacheManagersToClose = cacheManagers.remove(classLoader);
            if (cacheManagersToClose != null)
               close(cacheManagersToClose);
         }
      }
   }

   private void close(Map<URI, JCacheManager> map) throws CachingShutdownException {
      IdentityHashMap<CacheManager, Exception> failures = new IdentityHashMap<CacheManager, Exception>();
      for (CacheManager cacheManager : map.values()) {
         try {
            cacheManager.close();

            if (log.isTraceEnabled())
               log.tracef("Shutdown cache manager '%s'", cacheManager.getURI());
         } catch (Exception e) {
            failures.put(cacheManager, e);
         }
      }
      if (!failures.isEmpty()) {
         throw new CachingShutdownException(failures);
      }
   }

   private JCacheManager createCacheManager(
         ClassLoader classLoader, URI uri) {
      return new JCacheManager(uri, classLoader, this);
   }

}
