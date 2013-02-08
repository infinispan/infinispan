/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
package org.infinispan.jcache;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.WeakHashMap;

import javax.cache.CacheManager;
import javax.cache.CacheManagerFactory;
import javax.cache.CachingShutdownException;

public class JCacheManagerFactory implements CacheManagerFactory {

   private static final Log log = LogFactory.getLog(
         JCacheManagerFactory.class);

   private static final JCacheManagerFactory INSTANCE =
         new JCacheManagerFactory();

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
    * get cache manager references via {@link javax.cache.Caching#getCacheManager()}.
    *
    * So, the only possible way to avoid leaking cache managers is to have a
    * weak key hash map keyed on class loader. So when no other hard
    * references to the class loader are kept, the cache manager can be
    * garbage collected and its {@link #finalize()} method can be called
    * if the user forgot to shut down the cache manager.
    */
   private final Map<ClassLoader, Map<String, JCacheManager>> cacheManagers =
           new WeakHashMap<ClassLoader, Map<String, JCacheManager>>();

   private JCacheManagerFactory() {
   }

   @Override
   public CacheManager getCacheManager(String name) {
      // Infinispan's global configuration builder already has a default
      // cache loader, but we need to specify one here in case the name
      // points to a file in the class path.
      return getCacheManager(
            Thread.currentThread().getContextClassLoader(), name);
   }

   @Override
   public CacheManager getCacheManager(ClassLoader classLoader, String name) {
      if (classLoader == null)
         throw new NullPointerException("Invalid classloader specified " + classLoader);

      if (name == null)
         throw new NullPointerException("Invalid cache name specified " + name);

      synchronized (cacheManagers) {
         Map<String, JCacheManager> map = cacheManagers.get(classLoader);
         if (map == null) {
            if (log.isTraceEnabled())
               log.tracef("No cache managers registered under '%s'", name);

            map = new HashMap<String, JCacheManager>();
            cacheManagers.put(classLoader, map);
         }

         JCacheManager cacheManager= map.get(name);
         if (cacheManager == null) {
            // Not found, create cache manager and add to collection
            cacheManager = createCacheManager(classLoader, name);
            if (log.isTraceEnabled())
               log.tracef("Created '%s' cache manager", name);

            map.put(name, cacheManager);
         }

         return cacheManager;
      }
   }

   @Override
   public void close() throws CachingShutdownException {
      synchronized (cacheManagers) {
         Map<CacheManager, Exception> failures =
               new IdentityHashMap<CacheManager, Exception>();
         for (Map<String, JCacheManager> map : cacheManagers.values()) {
            try {
               shutdown(map);
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
   public boolean close(ClassLoader classLoader) throws CachingShutdownException {
      return close(classLoader, null);
   }

   @Override
   public boolean close(ClassLoader classLoader, String name)
         throws CachingShutdownException {
      synchronized (cacheManagers) {
         if (name != null) {
            Map<String, JCacheManager> map = cacheManagers.get(classLoader);
            JCacheManager cacheManager = map.remove(name);
            if (map.isEmpty())
               cacheManagers.remove(classLoader);

            if (cacheManager == null)
               return false;

            cacheManager.shutdown();
            return true;
         } else {
            Map<String, JCacheManager> map = cacheManagers.remove(classLoader);
            if (map == null)
               return false;

            shutdown(map);
            return true;
         }
      }
   }

   public static JCacheManagerFactory getInstance() {
      return INSTANCE;
   }

   private void shutdown(Map<String, JCacheManager> map) throws CachingShutdownException {
      IdentityHashMap<CacheManager, Exception> failures = new IdentityHashMap<CacheManager, Exception>();
      for (CacheManager cacheManager : map.values()) {
         try {
            cacheManager.shutdown();

            if (log.isTraceEnabled())
               log.tracef("Shutdown cache manager '%s'", cacheManager.getName());
         } catch (Exception e) {
            failures.put(cacheManager, e);
         }
      }
      if (!failures.isEmpty()) {
         throw new CachingShutdownException(failures);
      }
   }

   private JCacheManager createCacheManager(
         ClassLoader classLoader, String name) {
      return new JCacheManager(name, classLoader);
   }

}
