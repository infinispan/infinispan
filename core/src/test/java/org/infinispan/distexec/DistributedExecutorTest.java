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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.testng.annotations.Test;

/**
 * Tests org.infinispan.distexec.DistributedExecutorService
 * 
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distexec.DistributedExecutorTest")
public class DistributedExecutorTest extends BaseDistFunctionalTest {

   public DistributedExecutorTest() {
   }

   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
   }

   public void testBasicInvocation() throws Exception {

      DistributedExecutorService des = new DefaultExecutorService(c1);
      Future<Integer> future = des.submit(new SimpleCallable());

      Integer r = future.get();
      assert r == 1;
   } 
   
   public void testExceptionInvocation() throws Exception {

      DistributedExecutorService des = new DefaultExecutorService(c1);

      Future<Integer> future = des.submit(new ExceptionThrowingCallable());
      int exceptionCount = 0;
      try {
         future.get();
         throw new IllegalStateException("Should not have reached this code");
      } catch (ExecutionException ex) {
         exceptionCount++;
      }
      assert exceptionCount == 1;

      List<Future<Integer>> list = des.submitEverywhere(new ExceptionThrowingCallable());
      exceptionCount = 0;
      for (Future<Integer> f : list) {
         try {
            f.get();
            throw new IllegalStateException("Should not have reached this code");
         } catch (ExecutionException ex) {
            exceptionCount++;
         }
      }
      assert exceptionCount == list.size();
   }

   public void testRunnableInvocation() throws Exception {

      DistributedExecutorService des = new DefaultExecutorService(c1);

      Future<?> future = des.submit(new BoringRunnable());
      Object object = future.get();
      assert object == null;

      des.execute(new BoringRunnable());
      int exceptionCount = 0;
      try {
         des.execute(new Runnable() {
            @Override
            public void run() {
            }
         });
         throw new Exception("Should not have happened");
      } catch (IllegalArgumentException iae) {
         exceptionCount++;
      }

      assert exceptionCount == 1;
   }
   
   public void testInvokeAny() throws Exception {

      DistributedExecutorService des = new DefaultExecutorService(c1);

      List<SimpleCallable> tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      Integer result = des.invokeAny(tasks);
      assert result == 1;
      
      tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());
      result = des.invokeAny(tasks);
      assert result == 1;
   }
   
   public void testInvokeAll() throws Exception {

      DistributedExecutorService des = new DefaultExecutorService(c1);

      List<SimpleCallable> tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      List<Future<Integer>> list = des.invokeAll(tasks);
      assert list.size() == 1;
      Future<Integer> future = list.get(0);
      assert future.get() == 1;
      
      tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());
      
      list = des.invokeAll(tasks);
      assert list.size() == 3;
      for (Future<Integer> f : list) {
         assert f.get() == 1;
      }            
   }
   
   /**
    * Tests Callable isolation as it gets invoked across the cluster
    * https://issues.jboss.org/browse/ISPN-1041
    * 
    * @throws Exception
    */
   public void testCallableIsolation() throws Exception {
      DefaultExecutorService des = new DefaultExecutorService(c1);

      List<Future<Integer>> list = des.submitEverywhere(new SimpleCallableWithField());
      assert list != null && !list.isEmpty();
      for (Future<Integer> f : list) {
         assert f.get() == 0 ;
      }
   }

   public void testTaskCancellation() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c2);
      Future<Integer> future = des.submit(new SimpleCallable());
      if (future.cancel(true)){
         assert future.isCancelled();
      } 
      assert future.isDone();      
   }

   public void testBasicDistributedCallable() throws Exception {

      DistributedExecutorService des = new DefaultExecutorService(c2);
      Future<Boolean> future = des.submit(new SimpleDistributedCallable(false));
      Boolean r = future.get();
      assert r;
   }

   public void testBasicDistributedCallableWitkKeys() throws Exception {
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DistributedExecutorService des = new DefaultExecutorService(c1);

      Future<Boolean> future = des.submit(new SimpleDistributedCallable(true), new String[] {
               "key1", "key2" });
      Boolean r = future.get();
      assert r;
   }

   public void testDistributedCallableEverywhereWithKeys() throws Exception {
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DefaultExecutorService des = new DefaultExecutorService(c1);

      List<Future<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(true),
               new String[] { "key1", "key2" });
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }
   }

   public void testDistributedCallableEverywhere() throws Exception {

      DefaultExecutorService des = new DefaultExecutorService(c1);

      List<Future<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(false));
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }
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

      @Override
      public Integer call() throws Exception {
         return 1;
      }
   }
   
   static class SimpleCallableWithField implements Callable<Integer>, Serializable {
      
      /** The serialVersionUID */
      private static final long serialVersionUID = -6262148927734766558L;
      private int count; 

      @Override
      public Integer call() throws Exception {
         return count++;
      }
   }

   static class ExceptionThrowingCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8682463816319507893L;

      @Override
      public Integer call() throws Exception {
         throw new Exception("Intentional Exception from ExceptionThrowingCallable");
      }
   }

   static class BoringRunnable implements Runnable, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = 6898519516955822402L;

      @Override
      public void run() {
         System.out.println("I am a boring runnable");
      }

   }
}
