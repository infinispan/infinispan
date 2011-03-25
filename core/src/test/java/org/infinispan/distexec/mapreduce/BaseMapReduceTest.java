/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
import org.infinispan.config.Configuration;
import org.infinispan.distexec.mapreduce.Collator;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * 
 * 
 * @author Vladimir Blagojevic
 */
@Test
public abstract class BaseMapReduceTest extends MultipleCacheManagersTest {

   public BaseMapReduceTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected Configuration.CacheMode getCacheMode() {
      return Configuration.CacheMode.DIST_SYNC;
   }
   
   protected String cacheName(){
      return "mapreducecache";
   }

   public void testinvokeMapReduce() throws Exception {
      Cache c1 = cache(0, cacheName());
      Cache c2 = cache(1, cacheName());

      c1.put("1", new Integer(1));
      c2.put("2", new Integer(2));
      c1.put("3", new Integer(3));
      c2.put("4", new Integer(4));
      c1.put("12", new Integer(12));
      c2.put("13", new Integer(13));
      c1.put("14", new Integer(14));
      c2.put("15", new Integer(15));

      c1.put("111", new Integer(111));
      c2.put("112", new Integer(112));
      c1.put("113", new Integer(113));
      c2.put("114", new Integer(114));
      c1.put("211", new Integer(211));
      c2.put("212", new Integer(212));
      c1.put("213", new Integer(213));
      c2.put("214", 214);

      MapReduceTask<String, Integer, Integer, Integer> t = new MapReduceTask<String, Integer, Integer, Integer>(c1);
      Integer r = t.mappedWith(new BasicMapper()).reducedWith(new BasicReducer())
               .collate(new Collator<Integer>() {

                  Integer result = 0;

                  @Override
                  public void reducedResultReceived(Address remoteNode, Integer remoteResult) {
                     log.info("reducedResultReceived from  " + remoteNode + ":" + remoteResult);
                     result += remoteResult != null ? remoteResult : 0;
                  }

                  @Override
                  public Integer collate() {
                     return result;
                  }
               });
      
      assert r == 1364;
   }

   private static class BasicMapper implements Mapper<String, Integer, Integer> {

      protected transient final Log log = LogFactory.getLog(getClass());

      @Override
      public Integer map(String key, Integer value) {
         log.info("Mapped key " + key + " with " + value);
         return value;
      }

      public String toString() {
         return "BasicMapper";
      }
   }

   private static class BasicReducer implements Reducer<Integer, Integer> {

      protected transient final Log log = LogFactory.getLog(getClass());

      public String toString() {
         return "BasicReducer";
      }

      @Override
      public Integer reduce(Integer mapResult, Integer previouslyReduced) {
         log.info("Reduced " + mapResult + ", prevReduced" + previouslyReduced);
         return previouslyReduced != null ? mapResult + previouslyReduced : mapResult;
      }
   }
}
