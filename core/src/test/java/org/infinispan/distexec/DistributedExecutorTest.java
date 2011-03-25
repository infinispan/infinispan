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
package org.infinispan.distexec;

import java.io.Serializable;
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
      try {
         future.get();
         throw new IllegalStateException("Should not have reached this code");
      } catch (ExecutionException ex) {
         System.out.println("Received exception as expected: " + ex.getLocalizedMessage());
      }
   }

   public void testRunnableInvocation() throws Exception {
      
      DistributedExecutorService des = new DefaultExecutorService(c1);

      Future<?> future = des.submit(new BoringRunnable());
      Object object = future.get();
      assert object == null;

      des.execute(new BoringRunnable());
      System.out.println("Invoking ExecutionService#execute - OK");

      try {
         des.execute(new Runnable() {
            @Override
            public void run() {
            }
         });
         throw new Exception("Should not have happened");
      } catch (IllegalArgumentException iae) {
         System.out.println("Non serializable Runnable submitted - OK");
      }
   }

   public void testTaskCancellation() throws Exception {
    
      DistributedExecutorService des = new DefaultExecutorService(c2);

      Future<Integer> future = des.submit(new SimpleCallable());
      future.cancel(true);
      try {
         future.get();
         throw new IllegalStateException("Should not have reached this code");
      } catch (Exception e) {
         assert e instanceof CancellationException;
      }
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

      Future<Boolean> future = des.submit(new SimpleDistributedCallable(true), new String[] { "key1", "key2" });
      Boolean r = future.get();
      assert r;
   }

   public void testDistributedCallableEverywhereWithKeys() throws Exception {
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DefaultExecutorService des = new DefaultExecutorService(c1);

      List<Future<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(true), new String[] { "key1", "key2" });
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

      @Override
      public Integer call() throws Exception {
         return 1;
      }
   }

   static class ExceptionThrowingCallable implements Callable<Integer>, Serializable {

      @Override
      public Integer call() throws Exception {
         throw new Exception("Intenttional Exception from ExceptionThrowingCallable");
      }
   }

   static class BoringRunnable implements Runnable, Serializable {

      @Override
      public void run() {
         System.out.println("I am a boring runnable");
      }

   }
}
