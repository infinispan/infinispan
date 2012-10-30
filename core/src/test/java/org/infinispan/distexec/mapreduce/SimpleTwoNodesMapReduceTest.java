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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * SimpleTwoNodesMapReduceTest tests Map/Reduce functionality using two Infinispan nodes and local
 * reduce
 * 
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@Test(groups = "functional", testName = "distexec.SimpleTwoNodesMapReduceTest")
public class SimpleTwoNodesMapReduceTest extends BaseWordCountMapReduceTest {
   
   
   private static AtomicInteger counter = new AtomicInteger();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(2, cacheName(), builder);
   }
   
   /**
    * This test is here intentionally as we can not share static counter variable among concurrently
    * executing subclasses of BaseWordCountMapReduceTest in our testsuite
    * 
    */
   public void testInvokeMapperCancellation() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null,
               new LatchMapper(), new WordCountReducer());
      final Future<Map<String, Integer>> future = task.executeAsynchronously();
      Future<Boolean> cancelled = fork(new Callable<Boolean>() {

         @Override
         public Boolean call() throws Exception {
            //make sure that all nodes receive the command and...
            eventually(new Condition() {
               
               @Override
               public boolean isSatisfied() throws Exception {
                  return counter.get() >= nodeCount();
               }
            });
            //...are ready to be canceled
            return future.cancel(true);
         }
      });
      boolean mapperCancelled = false;
      Throwable root = null;
      try {
         future.get();
      } catch (Exception e) { 
         root = e;
         while(root.getCause() != null){
            root = root.getCause();
         }         
         mapperCancelled = root.getClass().equals(RuntimeException.class);         
      }
      assert mapperCancelled : "Mapper not cancelled, root cause " + root;
      assert cancelled.get();
   }
   
   static class LatchMapper implements Mapper<String, String, String, Integer> {

      /** The serialVersionUID */
      private static final long serialVersionUID = 2518908878377582179L;      
      
      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         boolean interrupted = false;
         CountDownLatch latch = new CountDownLatch(1);
         try {
            if (!interrupted) {
               counter.incrementAndGet();
               latch.await(5000, TimeUnit.MILLISECONDS);
            } else {
               interrupted = true;// already interrupted
            }
         } catch (InterruptedException e) {
            interrupted = true;
            Thread.currentThread().interrupt();
         }
         //as we can not throw InterruptedException 
         //throw a RuntimeException and check for it in the test...         
         if (interrupted) throw new RuntimeException();
      }
   }
}
