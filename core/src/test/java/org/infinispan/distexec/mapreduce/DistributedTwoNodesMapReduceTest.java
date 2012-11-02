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
import org.infinispan.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map;

/**
 * DistributedTwoNodesMapReduceTest tests Map/Reduce functionality using two Infinispan nodes,
 * distributed reduce and individual per task intermediate key/value cache
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@Test(groups = "functional", testName = "distexec.DistributedTwoNodesMapReduceTest", enabled = false, description = "Re:enable with https://issues.jboss.org/browse/ISPN-2439")
public class DistributedTwoNodesMapReduceTest extends BaseWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(2, cacheName(), builder);
   }
   
   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c){
      //run distributed reduce with per task cache
      return new MapReduceTask<String, String, String, Integer>(c, true, false);
   }



   //todo - everything below this line should be removed once ISPN-2439 is integrated. These have been added simply
   // because testNG is a PITA and disabling this test simply doesn't make testNG not run it (phew!).

   @Test(enabled = false, description = "Re:enable with https://issues.jboss.org/browse/ISPN-2439")
   public void testImproperCacheStateForMapReduceTask(){}

   @Override
   public void testinvokeMapReduceOnAllKeys() throws Exception {
      super.testinvokeMapReduceOnAllKeys();
   }

   @Override
   public void testinvokeMapReduceOnEmptyKeys() throws Exception {
      super.testinvokeMapReduceOnEmptyKeys();
   }

   @Override
   public void testCombinerForDistributedReductionWithException() throws Exception {
      super.testCombinerForDistributedReductionWithException();
   }

   @Override
   public void testinvokeMapReduceOnAllKeysWithCombiner() throws Exception {
      super.testinvokeMapReduceOnAllKeysWithCombiner();
   }

   @Override
   public void testCombinerDoesNotChangeResult() throws Exception {
      super.testCombinerDoesNotChangeResult();
   }

   @Override
   public void testMapperReducerIsolation() throws Exception {
      super.testMapperReducerIsolation();
   }

   @Override
   public void testinvokeMapReduceOnAllKeysAsync() throws Exception {
      super.testinvokeMapReduceOnAllKeysAsync();
   }

   @Override
   public void testinvokeMapReduceOnSubsetOfKeys() throws Exception {
      super.testinvokeMapReduceOnSubsetOfKeys();
   }

   @Override
   public void testinvokeMapReduceOnSubsetOfKeysAsync() throws Exception {
      super.testinvokeMapReduceOnSubsetOfKeysAsync();
   }

   @Override
   protected void verifyResults(Map<String, Integer> result, Map<String, Integer> verifyAgainst) {
      super.verifyResults(result, verifyAgainst);
   }

   @Override
   public void testinvokeMapReduceOnAllKeysWithCollator() throws Exception {
      super.testinvokeMapReduceOnAllKeysWithCollator();
   }

   @Override
   public void testinvokeMapReduceOnSubsetOfKeysWithCollator() throws Exception {
      super.testinvokeMapReduceOnSubsetOfKeysWithCollator();
   }

   @Override
   public void testinvokeMapReduceOnAllKeysWithCollatorAsync() throws Exception {
      super.testinvokeMapReduceOnAllKeysWithCollatorAsync();
   }

   @Override
   public void testinvokeMapReduceOnSubsetOfKeysWithCollatorAsync() throws Exception {
      super.testinvokeMapReduceOnSubsetOfKeysWithCollatorAsync();
   }

}
