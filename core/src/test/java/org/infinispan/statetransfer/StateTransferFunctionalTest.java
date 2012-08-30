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
package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

@Test(groups = "functional", testName = "statetransfer.StateTransferFunctionalTest", enabled = true)
public class StateTransferFunctionalTest extends MultipleCacheManagersTest {

   public static final String A_B_NAME = "a_b_name";
   public static final String A_C_NAME = "a_c_name";
   public static final String A_D_NAME = "a_d_age";
   public static final String A_B_AGE = "a_b_age";
   public static final String A_C_AGE = "a_c_age";
   public static final String A_D_AGE = "a_d_age";
   public static final String JOE = "JOE";
   public static final String BOB = "BOB";
   public static final String JANE = "JANE";
   public static final Integer TWENTY = 20;
   public static final Integer FORTY = 40;

   Configuration config;
   protected final String cacheName;

   private volatile int testCount = 0;

   private static final Log log = LogFactory.getLog(StateTransferFunctionalTest.class);

   public StateTransferFunctionalTest() {
      this("nbst");
   }

   public StateTransferFunctionalTest(String testCacheName) {
      cacheName = testCacheName;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {
      // This impl only really sets up a configuration for use later.
      config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      config.setSyncReplTimeout(30000);
      config.setFetchInMemoryState(true);
      config.setUseLockStriping(false); // reduces the odd chance of a key collision and deadlock
   }

   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(new TransportFlags().withMerge(true));
      cm.defineConfiguration(cacheName, config.clone());
      return cm;
   }

   public static class DelayTransfer implements Serializable {

      private static final long serialVersionUID = 6361429803359702822L;
      
      private volatile boolean doDelay = false;

      private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
         in.defaultReadObject();
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();

         if (doDelay) {
            try {
               // Delay state transfer
               Thread.sleep(2000);
            }
            catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
      }

      public void enableDelay() {
         doDelay = true;
      }
   }

   private static class WritingThread extends Thread {
      private final Cache<Object, Object> cache;
      private final boolean tx;
      private volatile boolean stop;
      private volatile int result;
      private TransactionManager tm;

      WritingThread(Cache<Object, Object> cache, boolean tx) {
         super("WriterThread," + cache.getCacheManager().getAddress());
         this.cache = cache;
         this.tx = tx;
         if (tx) tm = TestingUtil.getTransactionManager(cache);
         setDaemon(true);
      }

      public int result() {
         return result;
      }

      public void run() {
         int c = 0;
         while (!stop) {
            boolean success = false;
            try {
               if (tx)
                  tm.begin();
               cache.put("test" + c, c++);
               if (tx)
                  tm.commit();
               success = true;
            } catch (Exception e) {
               c--;
               log.errorf("Error writing key test%s", c, e);
               stopThread();
            } finally {
               if (tx && !success) {
                  try {
                     tm.rollback();
                  } catch (SystemException e) {
                     log.error(e);
                  }
               }
            }
         }
         result = c;
      }

      public void stopThread() {
         stop = true;
      }
   }

   public void testInitialStateTransfer(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2;
      EmbeddedCacheManager cm1 = createCacheManager();
      cache1 = cm1.getCache(cacheName);
      writeInitialData(cache1);

      JoiningNode node = new JoiningNode(createCacheManager());
      cache2 = node.getCache(cacheName);
      node.waitForJoin(60000, cache1, cache2);
      node.verifyStateTransfer(new CacheVerifier(cache2));

      logTestEnd(m);
   }

   public void testInitialStateTransferCacheNotPresent(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2;
      EmbeddedCacheManager cacheManager1 = createCacheManager();
      cache1 = cacheManager1.getCache(cacheName);
      writeInitialData(cache1);

      JoiningNode node = new JoiningNode(createCacheManager());
      cache2 = node.getCache(cacheName);
      node.waitForJoin(60000, cache1, cache2);
      node.verifyStateTransfer(new CacheVerifier(cache2));

      cacheManager1.defineConfiguration("otherCache", config.clone());
      cacheManager1.getCache("otherCache");
      logTestEnd(m);
   }

   public void testConcurrentStateTransfer(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2, cache3, cache4;
      cache1 = createCacheManager().getCache(cacheName);
      writeInitialData(cache1);

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache2 = node2.getCache(cacheName);

      cache1.put("delay", new StateTransferFunctionalTest.DelayTransfer());

      node2.waitForJoin(60000, cache1, cache2);
      node2.verifyStateTransfer(new CacheVerifier(cache2));

      final JoiningNode node3 = new JoiningNode(createCacheManager());
      final JoiningNode node4 = new JoiningNode(createCacheManager());

      Thread t1 = new Thread(new Runnable() {
         public void run() {
            node3.getCache(cacheName);
         }
      });
      t1.setName("CacheStarter-Cache3");
      t1.start();

      Thread t2 = new Thread(new Runnable() {
         public void run() {
            node4.getCache(cacheName);
         }
      });
      t2.setName("CacheStarter-Cache4");
      t2.start();

      t1.join();
      t2.join();

      cache3 = node3.getCache(cacheName);
      cache4 = node4.getCache(cacheName);

      node3.waitForJoin(120000, cache1, cache2, cache3, cache4);
      node4.waitForJoin(120000, cache1, cache2, cache3, cache4);

      node3.verifyStateTransfer(new CacheVerifier(cache3));
      node4.verifyStateTransfer(new CacheVerifier(cache4));

      logTestEnd(m);
   }

   @Test(enabled = false, description = "See ISPN-2106")
   public void testSTWithThirdWritingNonTxCache(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      thirdWritingCacheTest(false);
      logTestEnd(m);
   }

   @Test(enabled = false, description = "See ISPN-2106")
   public void testSTWithThirdWritingTxCache(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      thirdWritingCacheTest(true);
      logTestEnd(m);
   }

   @Test (timeOut = 120000)
   public void testSTWithWritingNonTxThread(Method m) throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      testCount++;
      logTestStart(m);
      writingThreadTest(false);
      logTestEnd(m);
   }

   @Test (timeOut = 120000)
   public void testSTWithWritingTxThread(Method m) throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      testCount++;
      logTestStart(m);
      writingThreadTest(true);
      logTestEnd(m);
   }

   @Test(enabled = false, description = "The new state transfer doesn't work with cache or cache manager restarts (yet)")
   public void testInitialStateTransferAfterRestart(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2;
      cache1 = createCacheManager().getCache(cacheName);
      writeInitialData(cache1);

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache2 = node2.getCache(cacheName);
      node2.waitForJoin(60000, cache1, cache2);
      node2.verifyStateTransfer(new CacheVerifier(cache2));

      cache2.stop();
      cache2.start();

      verifyInitialData(cache2);
      logTestEnd(m);
   }

   private void logTestStart(Method m) {
      logTestLifecycle(m, "start");
   }

   private void logTestEnd(Method m) {
      logTestLifecycle(m, "end");
   }

   private void logTestLifecycle(Method m, String lifecycle) {
      log.infof("%s %s - %s", m.getName(), lifecycle, testCount);
   }

   private void thirdWritingCacheTest(boolean tx) throws Exception {
      Cache<Object, Object> cache1, cache2, cache3;
      cache1 = createCacheManager().getCache(cacheName);
      cache3 = createCacheManager().getCache(cacheName);
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache3);

      writeInitialData(cache1);

      // Delay the transient copy, so that we get a more thorough log test
      DelayTransfer value = new DelayTransfer();
      cache1.put("delay", value);
      value.enableDelay();

      WritingThread writerThread = new WritingThread(cache3, tx);
      writerThread.start();

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache2 = node2.getCache(cacheName);

      node2.waitForJoin(60000, cache1, cache2, cache3);

      writerThread.stopThread();
      writerThread.join();

      node2.verifyStateTransfer(new CacheVerifier(cache2));

      int count = writerThread.result();

      for (int c = 0; c < count; c++) {
         Object o = cache2.get("test" + c);
         assert new Integer(c).equals(o) : "Entry under key [test" + c + "] was [" + cache2.get("test" + c) + "] but expected [" + c + "]";
      }
   }

   protected void verifyInitialData(Cache<Object, Object> c) {
      Address address = c.getAdvancedCache().getRpcManager().getAddress();
      log.debugf("Checking values on cache " + address);
      assert JOE.equals(c.get(A_B_NAME)) : "Incorrect value for key " + A_B_NAME;
      assert TWENTY.equals(c.get(A_B_AGE)) : "Incorrect value for key " + A_B_AGE;
      assert BOB.equals(c.get(A_C_NAME)) : "Incorrect value for key " + A_C_NAME;
      assert FORTY.equals(c.get(A_C_AGE)) : "Incorrect value for key " + A_C_AGE;
   }

   protected void writeInitialData(final Cache<Object, Object> c) {
      c.put(A_B_NAME, JOE);
      c.put(A_B_AGE, TWENTY);
      c.put(A_C_NAME, BOB);
      c.put(A_C_AGE, FORTY);
   }

   private void writingThreadTest(boolean tx) throws Exception {
      Cache<Object, Object> cache1, cache2;
      cache1 = createCacheManager().getCache(cacheName);

      writeInitialData(cache1);
      // Delay the transient copy, so that we get a more thorough log test
      DelayTransfer value = new DelayTransfer();
      cache1.put("delay", value);
      value.enableDelay();

      WritingThread writerThread = new WritingThread(cache1, tx);
      writerThread.start();
      verifyInitialData(cache1);

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache2 = node2.getCache(cacheName);
      node2.waitForJoin(60000, cache1, cache2);

      writerThread.stopThread();
      writerThread.join();

      verifyInitialData(cache1);
      node2.verifyStateTransfer(new CacheVerifier(cache2));

      int count = writerThread.result();

      for (int c = 0; c < count; c++)
         assert new Integer(c).equals(cache2.get("test" + c)) : "Entry under key [test" + c + "] was [" + cache2.get("test" + c) + "] but expected [" + c + "]";
   }

   public class CacheVerifier implements Callable<Void> {

      private final Cache<Object, Object> cache;

      public CacheVerifier(Cache<Object, Object> cache) {
         this.cache = cache;
      }

      @Override
      public Void call() throws Exception {
         verifyInitialData(cache);
         return null;
      }

   }

}
