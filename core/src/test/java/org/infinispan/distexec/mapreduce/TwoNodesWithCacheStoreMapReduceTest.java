/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyStoreConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

/**
 * TwoNodesWithCacheStoreMapReduceTest tests Map/Reduce functionality using two Infinispan nodes,
 * local reduce and also to verify that having values in cache store only does not lead to any
 * additional key/value being missed for map/reduce algorithm
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@Test(groups = "functional", testName = "distexec.TwoNodesWithCacheStoreMapReduceTest")
public class TwoNodesWithCacheStoreMapReduceTest extends BaseWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      LegacyStoreConfigurationBuilder store = builder.loaders().addStore().cacheStore(new DummyInMemoryCacheStore(getClass().getSimpleName()));
      store.purgeOnStartup(true);
      createClusteredCaches(2, cacheName(), builder);
   }

   @Override
   @SuppressWarnings({ "rawtypes"})
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[],
            Mapper<String, String, String, Integer> mapper, Reducer<String, Integer> reducer, boolean useCombiner)
            throws Exception {
      Cache cache1 = cache(0, cacheName());
      Cache cache2 = cache(1, cacheName());

      CacheStore c1 = getStore(cache1);
      CacheStore c2 = getStore(cache2);

      //store each entry into each cache store directly
      c1.store(toEntry("1", "Hello world here I am"));
      c2.store(toEntry("1", "Hello world here I am"));
      c1.store(toEntry("2", "Infinispan rules the world"));
      c2.store(toEntry("2", "Infinispan rules the world"));
      c1.store(toEntry("3", "JUDCon is in Boston"));
      c2.store(toEntry("3", "JUDCon is in Boston"));
      c1.store(toEntry("4", "JBoss World is in Boston as well"));
      c2.store(toEntry("4", "JBoss World is in Boston as well"));
      c1.store(toEntry("12","JBoss Application Server"));
      c2.store(toEntry("12","JBoss Application Server"));
      c1.store(toEntry("15", "Hello world"));
      c2.store(toEntry("15", "Hello world"));
      c1.store(toEntry("14", "Infinispan community"));
      c2.store(toEntry("14", "Infinispan community"));

      c1.store(toEntry("111", "Infinispan open source"));
      c2.store(toEntry("111", "Infinispan open source"));
      c1.store(toEntry("112", "Boston is close to Toronto"));
      c2.store(toEntry("112", "Boston is close to Toronto"));
      c1.store(toEntry("113", "Toronto is a capital of Ontario"));
      c2.store(toEntry("113", "Toronto is a capital of Ontario"));
      c1.store(toEntry("114", "JUDCon is cool"));
      c2.store(toEntry("114", "JUDCon is cool"));
      c1.store(toEntry("211", "JBoss World is awesome"));
      c2.store(toEntry("211", "JBoss World is awesome"));
      c1.store(toEntry("212", "JBoss rules"));
      c2.store(toEntry("212", "JBoss rules"));
      c1.store(toEntry("213", "JBoss division of RedHat "));
      c2.store(toEntry("213", "JBoss division of RedHat "));
      c1.store(toEntry("214", "RedHat community"));
      c2.store(toEntry("214", "RedHat community"));

      MapReduceTask<String, String, String, Integer> task = createMapReduceTask(cache1);
      task.mappedWith(mapper).reducedWith(reducer);
      if(useCombiner)
         task.combinedWith(reducer);

      if(keys != null && keys.length>0){
         task.onKeys(keys);
      }
      return task;
   }

   @SuppressWarnings("rawtypes")
   protected CacheStore getStore(Cache c){
      return TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
   }

   protected InternalCacheEntry toEntry(String key, String value){
      return TestInternalCacheEntryFactory.create(key,value);
   }
}
