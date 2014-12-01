package org.infinispan.all.embedded.util;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractDelegatingMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Mini utils for infinispan-embedded integration tests.
 *
 * @author Tomas Sykora (tsykora@redhat.com)
 */
public class EmbeddedUtils {

   private static final Log log = LogFactory.getLog(EmbeddedUtils.class);

   public static void killCacheManagers(boolean clear, EmbeddedCacheManager... cacheManagers) {
      // stop the caches first so that stopping the cache managers doesn't trigger a rehash
      for (EmbeddedCacheManager cm : cacheManagers) {
         try {
            killCaches(clear, getRunningCaches(cm));
         } catch (Throwable e) {
            log.warn("Problems stopping cache manager " + cm, e);
         }
      }
      for (EmbeddedCacheManager cm : cacheManagers) {
         try {
            if (cm != null) cm.stop();
         } catch (Throwable e) {
            log.warn("Problems killing cache manager " + cm, e);
         }
      }
   }

   /**
    * Kills a cache - stops it and rolls back any associated txs
    */
   public static void killCaches(boolean clear, Collection<Cache> caches) {
      for (Cache c : caches) {
         try {
            if (c != null && c.getStatus() == ComponentStatus.RUNNING) {
               TransactionManager tm = c.getAdvancedCache().getTransactionManager();
               if (tm != null) {
                  try {
                     tm.rollback();
                  }
                  catch (Exception e) {
                     // don't care
                  }
               }
               if (c.getAdvancedCache().getRpcManager() != null) {
                  log.tracef("Cache contents on %s before stopping: %s", c.getAdvancedCache().getRpcManager().getAddress(), c.entrySet());
               } else {
                  log.tracef("Cache contents before stopping: %s", c.entrySet());
               }
               if (clear) {
                  try {
                     c.clear();
                  } catch (Exception ignored) {}
               }
               c.stop();
            }
         }
         catch (Throwable t) {
            log.tracef("Problems with killing caches: %s", t.getStackTrace());
         }
      }
   }

   public static Set<Cache> getRunningCaches(EmbeddedCacheManager cacheContainer) {
      Set<Cache> running = new HashSet<Cache>();
      if (cacheContainer == null || !cacheContainer.getStatus().allowInvocations())
         return running;

      for (String cacheName : cacheContainer.getCacheNames()) {
         if (cacheContainer.isRunning(cacheName)) {
            Cache c = cacheContainer.getCache(cacheName);
            if (c.getStatus().allowInvocations()) running.add(c);
         }
      }

      if (cacheContainer.isDefaultRunning()) {
         Cache defaultCache = cacheContainer.getCache();
         if (defaultCache.getStatus().allowInvocations()) running.add(defaultCache);
      }

      return running;
   }

   // for marshalling tests

   public static AbstractDelegatingMarshaller extractCacheMarshaller(Cache cache) {
      ComponentRegistry cr = (ComponentRegistry) extractField(cache, "componentRegistry");
      StreamingMarshaller marshaller = cr.getComponent(StreamingMarshaller.class, KnownComponentNames.CACHE_MARSHALLER);
      return (AbstractDelegatingMarshaller) marshaller;
   }

   public static <T> T extractComponent(Cache cache, Class<T> componentType) {
      ComponentRegistry cr = extractComponentRegistry(cache.getAdvancedCache());
      return cr.getComponent(componentType);
   }

   public static ComponentRegistry extractComponentRegistry(Cache cache) {
      ComponentRegistry cr = (ComponentRegistry) extractField(cache, "componentRegistry");
      if (cr == null) cr = cache.getAdvancedCache().getComponentRegistry();
      return cr;
   }

   public static <T> T extractField(Object target, String fieldName) {
      return (T) extractField(target.getClass(), target, fieldName);
   }

   public static Object extractField(Class type, Object target, String fieldName) {
      while (true) {
         Field field;
         try {
            field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
         } catch (Exception e) {
            if (type.equals(Object.class)) {
               e.printStackTrace();
               return null;
            } else {
               // try with superclass!!
               type = type.getSuperclass();
            }
         }
      }
   }
}
