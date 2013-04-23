/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.tasks;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;

/**
 * GlobalKeySetTask is a {@link DistributedCallable} for obtaining all of the keys
 * across a cluster.
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 5.3
 */
public class GlobalKeySetTask<K, V> implements DistributedCallable<K, V, Set<K>>, Serializable {
   private Cache<K, V> cache;

   @Override
   public Set<K> call() throws Exception {
      return new HashSet<K>(cache.keySet());
   }

   @Override
   public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
      this.cache = cache;
   }

   public static <K, V> Set<K> getGlobalKeySet(Cache<K, V> cache) throws InterruptedException, ExecutionException {
      if (cache.getCacheConfiguration().clustering().cacheMode().isDistributed()) {
         DefaultExecutorService des = new DefaultExecutorService(cache);
         List<Future<Set<K>>> fk = des.submitEverywhere(new GlobalKeySetTask<K, V>());
         Set<K> allKeys = new HashSet<K>();
         for(Future<Set<K>> f : fk) {
            allKeys.addAll(f.get());
         }
         return allKeys;
      } else {
         return new HashSet<K>(cache.keySet());
      }
   }
}
