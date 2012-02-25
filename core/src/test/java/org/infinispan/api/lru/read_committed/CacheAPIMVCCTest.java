/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.api.lru.read_committed;

import org.infinispan.api.CacheAPITest;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.lru.read_committed.CacheAPIMVCCTest")
public class CacheAPIMVCCTest extends CacheAPITest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.READ_COMMITTED;
   }

   @Override
   protected ConfigurationBuilder addEviction(ConfigurationBuilder cb) {
      cb
            .eviction()
               .strategy(EvictionStrategy.LRU)
               .maxEntries(1000)
            .expiration()
               .wakeUpInterval(60000);
      return cb;
   }

   public void testRollbackAfterClear() throws Exception {
      String key = "key", value = "value";
      int size = 0;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      cache.clear();
      assert cache.get(key) == null;
      size = 0;
      assert size == cache.size();
      assert size == cache.keySet().size();
      assert size == cache.values().size();
      assert size == cache.entrySet().size();
      TestingUtil.getTransactionManager(cache).rollback();

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

}
