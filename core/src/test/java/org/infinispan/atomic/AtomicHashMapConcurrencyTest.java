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
package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Tester class for AtomicMapCache.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "atomic.AtomicHashMapConcurrencyTest")
public class AtomicHashMapConcurrencyTest extends AbstractInfinispanTest {

   public static final String KEY = "key";
   Cache<String, Object> cache;
   TransactionManager tm;
   private CacheContainer cm;

   enum Operation {
      PUT,
      COMMIT,
      READ
   }

   @BeforeMethod
   @SuppressWarnings("unchecked")
   public void setUp() {
      Configuration c = new Configuration();
      c.setLockAcquisitionTimeout(500);
      // these 2 need to be set to use the AtomicMapCache
      c.setInvocationBatchingEnabled(true);
      cm = TestCacheManagerFactory.createCacheManager(c, true);
      cache = cm.getCache();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   public void tearDown() {
      try {
         tm.rollback();
      } catch (Exception e) {
      }
      TestingUtil.killCacheManagers(cm);
   }

   public void testConcurrentCreate() throws Exception {
      tm.begin();
      AtomicMapLookup.getAtomicMap(cache, KEY);
      OtherThread ot = new OtherThread();
      ot.start();
      Object response = ot.response.take();
      assert response instanceof TimeoutException;
   }

   public void testConcurrentModifications() throws Exception {
      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      tm.begin();
      atomicMap.put(1, "");
      OtherThread ot = new OtherThread();
      ot.start();
      ot.toExecute.put(Operation.PUT);
      Object response = ot.response.take();
      assert response instanceof TimeoutException;
   }

   public void testReadAfterTxStarted() throws Exception {
      OtherThread ot = new OtherThread();
      try {
         AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
         atomicMap.put(1, "existing");
         tm.begin();
         atomicMap.put(1, "newVal");
         ot.start();
         ot.toExecute.put(Operation.READ);
         Object response = ot.response.take();
         assert response.equals("existing");
         tm.commit();
         assert atomicMap.get(1).equals("newVal");
         ot.toExecute.put(Operation.READ);
         response = ot.response.take();
         assert response.equals("newVal");
      } finally {
         ot.interrupt();
      }
   }

   public class OtherThread extends Thread {

      public OtherThread() {
         super("OtherThread");
      }

      BlockingQueue<Object> response = new ArrayBlockingQueue<Object>(1);

      BlockingQueue<Operation> toExecute = new ArrayBlockingQueue<Operation>(1);

      @Override
      public void run() {
         try {
            tm.begin();
            AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
            boolean notCommited = true;
            while (notCommited) {
               Operation op = toExecute.take();
               switch (op) {
                  case PUT: {
                     atomicMap.put(1, "val");
                     response.put(new Object());
                     break;
                  }
                  case READ: {
                     String val = atomicMap.get(1);
                     response.put(String.valueOf(val));
                     break;
                  }
                  case COMMIT: {
                     tm.commit();
                     response.put(new Object());
                     notCommited = false;
                     break;
                  }
               }
            }
         } catch (Exception e) {
            try {
               response.put(e);
            } catch (InterruptedException e1) {
               e1.printStackTrace();
            }
            e.printStackTrace();
         }
      }
   }
}
