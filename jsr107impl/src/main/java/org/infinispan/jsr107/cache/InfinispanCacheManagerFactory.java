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
package org.infinispan.jsr107.cache;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import javax.cache.CacheManager;
import javax.cache.CacheManagerFactory;
import javax.cache.CachingShutdownException;

public class InfinispanCacheManagerFactory implements CacheManagerFactory {

   private static final InfinispanCacheManagerFactory INSTANCE = new InfinispanCacheManagerFactory();

   private final Map<ClassLoader, Map<String, CacheManager>> cacheManagers = new HashMap<ClassLoader, Map<String, CacheManager>>();

   private InfinispanCacheManagerFactory() {
   }

   @Override
   public CacheManager getCacheManager(String name) {
      return getCacheManager(getDefaultClassLoader(), name);
   }

   @Override
   public CacheManager getCacheManager(ClassLoader classLoader, String name) {
      if (classLoader == null) {
         throw new NullPointerException("Invalid classloader specified " + classLoader);
      }
      if (name == null) {
         throw new NullPointerException("Invalid cache name specified " + name);
      }
      synchronized (cacheManagers) {
         Map<String, CacheManager> map = cacheManagers.get(classLoader);
         if (map == null) {
            map = new HashMap<String, CacheManager>();
            cacheManagers.put(classLoader, map);
         }
         CacheManager cacheManager = map.get(name);
         if (cacheManager == null) {
            cacheManager = createCacheManager(classLoader, name);
            map.put(name, cacheManager);
         }
         return cacheManager;
      }
   }

   @Override
   public void close() throws CachingShutdownException {
      synchronized (cacheManagers) {
         IdentityHashMap<CacheManager, Exception> failures = new IdentityHashMap<CacheManager, Exception>();
         for (Map<String, CacheManager> cacheManagerMap : cacheManagers.values()) {
            try {
               shutdown(cacheManagerMap);
            } catch (CachingShutdownException e) {
               failures.putAll(e.getFailures());
            }
         }
         cacheManagers.clear();
         if (!failures.isEmpty()) {
            throw new CachingShutdownException(failures);
         }
      }
   }

   @Override
   public boolean close(ClassLoader classLoader) throws CachingShutdownException {
      Map<String, CacheManager> cacheManagerMap;
      synchronized (cacheManagers) {
         cacheManagerMap = cacheManagers.remove(classLoader);
      }
      if (cacheManagerMap == null) {
         return false;
      } else {
         shutdown(cacheManagerMap);
         return true;
      }
   }

   @Override
   public boolean close(ClassLoader classLoader, String name) throws CachingShutdownException {
      CacheManager cacheManager;
      synchronized (cacheManagers) {
         Map<String, CacheManager> cacheManagerMap = cacheManagers.get(classLoader);
         cacheManager = cacheManagerMap.remove(name);
         if (cacheManagerMap.isEmpty()) {
            cacheManagers.remove(classLoader);
         }
      }
      if (cacheManager == null) {
         return false;
      } else {
         cacheManager.shutdown();
         return true;
      }
   }

   public static InfinispanCacheManagerFactory getInstance() {
      return INSTANCE;
   }

   private void shutdown(Map<String, CacheManager> cacheManagerMap) throws CachingShutdownException {
      IdentityHashMap<CacheManager, Exception> failures = new IdentityHashMap<CacheManager, Exception>();
      for (CacheManager cacheManager : cacheManagerMap.values()) {
         try {
            cacheManager.shutdown();
         } catch (Exception e) {
            failures.put(cacheManager, e);
         }
      }
      if (!failures.isEmpty()) {
         throw new CachingShutdownException(failures);
      }
   }

   private CacheManager createCacheManager(ClassLoader classLoader, String name) {
      return new InfinispanCacheManager(name, classLoader);
   }

   private ClassLoader getDefaultClassLoader() {
      return Thread.currentThread().getContextClassLoader();
   }
}
