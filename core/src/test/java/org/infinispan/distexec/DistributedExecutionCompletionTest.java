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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.testng.annotations.Test;

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
}
