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
package org.infinispan.distexec;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests org.infinispan.distexec.DistributedExecutorService
 * 
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distexec.DistributedExecutionCompletionTest")
public class DistributedExecutionCompletionTest extends BaseDistFunctionalTest {

   public DistributedExecutionCompletionTest() {
   }

   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
   }

   public void testBasicInvocation() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des); 
      decs.submit(new SimpleCallable());
      NotifyingFuture<Integer> future = decs.take();
      Integer r = future.get();
      assert r == 1;
   } 
   
  
   public void testBasicDistributedCallableWitkKeys() throws Exception {
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Boolean> decs = new DistributedExecutionCompletionService<Boolean>(des); 


      decs.submit(new SimpleDistributedCallable(true), new String[] {"key1", "key2" });
      Future<Boolean> future = decs.take(); 
      Boolean r = future.get();
      assert r;
   }

   public void testDistributedCallableEverywhereWithKeys() throws Exception {
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DefaultExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Boolean> decs = new DistributedExecutionCompletionService<Boolean>(des);
      decs.submitEverywhere(new SimpleDistributedCallable(true),new String[] { "key1", "key2" });
      TestingUtil.sleepThread(1000);
      Future<Boolean> f = null;
      int counter = 0;
      while ((f = decs.poll()) != null) {
         assert f.get();
         counter++;
      }
      
      assert counter > 0;
   }

   public void testDistributedCallableEverywhere() throws Exception {

      DefaultExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Boolean> decs = new DistributedExecutionCompletionService<Boolean>(des);

      decs.submitEverywhere(new SimpleDistributedCallable(false));
      TestingUtil.sleepThread(1000);
      Future<Boolean> f = null;
      int counter = 0;
      while ((f = decs.poll()) != null) {
         assert f.get();
         counter ++;
      }
      assert counter > 0;
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicInvocationWithNullExecutor() throws Exception {
      DistributedExecutorService des = null;
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicInvocationWithNullTask() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);

      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
      decs.submit(null);
   }

   public void testBasicInvocationWithBlockingQueue() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      BlockingQueue<NotifyingFuture<Integer>> queue = new ArrayBlockingQueue<NotifyingFuture<Integer>>(10);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des, queue);
      decs.submit(new SimpleCallable());
      NotifyingFuture<Integer> future = decs.take();
      Integer r = future.get();
      AssertJUnit.assertEquals((Integer) 1, r);
   }

   public void testBasicInvocationWithRunnable() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);

      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
      Integer result = 5;
      decs.submit(new SimpleRunnable(), result);

      NotifyingFuture<Integer> future = decs.take();
      Integer r = future.get();
      AssertJUnit.assertEquals(result, r);
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicInvocationWithNullRunnable() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);

      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
      Integer result = 5;
      decs.submit(null, result);
   }

   public void testBasicPollInvocationWithSleepingCallable() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);

      decs.submit(new SimpleCallable(true));
      NotifyingFuture<Integer> callable = decs.poll();

      AssertJUnit.assertNull(callable);
   }

   public void testBasicTakeInvocationWithSleepingCallable() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);

      decs.submit(new SimpleCallable(true));

      long start = System.currentTimeMillis();
      NotifyingFuture<Integer> callable = decs.take();
      long end = System.currentTimeMillis();

      assert (end - start) >= 10000;
      AssertJUnit.assertEquals((Integer) 1, callable.get());
   }

   public void testBasicPollInvocation() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);

      decs.submit(new SimpleCallable());

      NotifyingFuture<Integer> callable = decs.poll(10, TimeUnit.MILLISECONDS);

      AssertJUnit.assertEquals((Integer) 1, callable.get());
   }

   public void testBasicPollInvocationWithTimeout() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);

      decs.submit(new SimpleCallable(true));
      NotifyingFuture<Integer> callable = decs.poll(10, TimeUnit.MILLISECONDS);

      AssertJUnit.assertNull(callable);
   }

   static class SimpleDistributedCallable implements DistributedCallable<String, String, Boolean>,
            Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = 623845442163221832L;
      private boolean invokedProperly = false;
      private final boolean hasKeys;

      public SimpleDistributedCallable(boolean hasKeys) {
         this.hasKeys = hasKeys;
      }

      @Override
      public Boolean call() throws Exception {
         return invokedProperly;
      }

      @Override
      public void setEnvironment(Cache<String, String> cache, Set<String> inputKeys) {
         boolean keysProperlySet = hasKeys ? inputKeys != null && !inputKeys.isEmpty()
                  : inputKeys != null && inputKeys.isEmpty();
         invokedProperly = cache != null && keysProperlySet;
      }

      public boolean validlyInvoked() {
         return invokedProperly;
      }
   }

   static class SimpleCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;
      private boolean shouldSleep = false;

      public SimpleCallable() {

      }

      public SimpleCallable(boolean shouldSleep) {
         this.shouldSleep = shouldSleep;
      }

      @Override
      public Integer call() throws Exception {
         if(shouldSleep) {
            Thread.sleep(10000);
         }

         return 1;
      }
   }

   static class SimpleRunnable implements Runnable, Serializable {

      @Override
      public void run() {
         System.out.println("This is a runnable!");
      }
   }
}
