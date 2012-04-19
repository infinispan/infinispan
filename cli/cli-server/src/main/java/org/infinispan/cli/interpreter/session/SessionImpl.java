/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
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
package org.infinispan.cli.interpreter.session;

import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.batch.BatchContainer;
import org.infinispan.manager.EmbeddedCacheManager;

public class SessionImpl implements Session {
   final EmbeddedCacheManager cacheManager;
   Cache<?, ?> cache;

   public SessionImpl(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public Cache<?, ?> getCache() {
      if (cache == null) {
         cache = cacheManager.getCache();
      }
      return cache;
   }

   @Override
   public Cache<?, ?> getCache(String cacheName) {
      Cache<Object, Object> c = cacheManager.getCache(cacheName, false);
      if (c == null) {
         throw new IllegalArgumentException("No cache named " + cacheName);
      }
      return c;
   }

   @Override
   public void setCacheName(String cacheName) {
      cache = getCache(cacheName);
   }

   @Override
   public void reset() {
      resetCache(cacheManager.getCache());
      for (String cacheName : cacheManager.getCacheNames()) {
         resetCache(cacheManager.getCache(cacheName));
      }
   }

   private void resetCache(Cache<Object, Object> cache) {
      if (cache.getCacheConfiguration().invocationBatching().enabled()) {
         cache.endBatch(false);
      }
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      try {
         if (tm.getTransaction() != null) {
            tm.rollback();
         }
      } catch (Exception e) {
      }
   }
}
