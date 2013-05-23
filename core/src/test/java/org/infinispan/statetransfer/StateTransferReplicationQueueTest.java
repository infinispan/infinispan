/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * State transfer and replication queue test verifying that the interaction between them two works in fine.
 *
 * @author Galder Zamarre�o
 * @since 4.1
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferReplicationQueueTest")
public class StateTransferReplicationQueueTest extends MultipleCacheManagersTest {

   public static final String A_B_NAME = "a_b_name";
   public static final String A_C_NAME = "a_c_name";
   public static final String A_B_AGE = "a_b_age";
   public static final String A_C_AGE = "a_c_age";
   public static final String JOE = "JOE";
   public static final String BOB = "BOB";
   public static final Integer TWENTY = 20;
   public static final Integer FORTY = 40;
   
   private final String cacheName = "nbst-replqueue";

   private ConfigurationBuilder config;

   protected void createCacheManagers() throws Throwable {
      // This impl only really sets up a configuration for use later.
      config = getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, true);
      config.clustering()
            .async().useReplQueue(true)
            .replQueueInterval(100)
            .replQueueMaxElements(100)
            .asyncMarshalling(true)
            .stateTransfer().fetchInMemoryState(true)
            .locking().useLockStriping(false); // reduces the odd chance of a key collision and deadlock
   }

   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(cacheName, config.build());
      return cm;
   }

   protected void writeInitialData(final Cache<Object, Object> c) {
      c.put(A_B_NAME, JOE);
      c.put(A_B_AGE, TWENTY);
      c.put(A_C_NAME, BOB);
      c.put(A_C_AGE, FORTY);
   }

   protected void verifyInitialData(Cache<Object, Object> c) {
      assert JOE.equals(c.get(A_B_NAME)) : "Incorrect value for key " + A_B_NAME;
      assert TWENTY.equals(c.get(A_B_AGE)) : "Incorrect value for key " + A_B_AGE;
      assert BOB.equals(c.get(A_C_NAME)) : "Incorrect value for key " + A_C_NAME;
      assert FORTY.equals(c.get(A_C_AGE)) : "Incorrect value for key " + A_C_AGE;
   }

   /**
    * In particular, this test focuses on checking that ordering is maintained when multiple operations are executed
    * on the same key in a asynchronous environment with async marshalling turned off.
    */
   public void testStateTransferWithNodeRestartedAndBusy(Method m) throws Exception {
      log.info(m.getName() + " start");
      thirdWritingCacheTest(false);
      log.info(m.getName() + "end");
   }

   private void thirdWritingCacheTest(boolean tx) throws InterruptedException {
      Cache<Object, Object> cache1, cache3;
      cache1 = createCacheManager().getCache(cacheName);
      EmbeddedCacheManager manager3 = createCacheManager();
      cache3 = manager3.getCache(cacheName);

      writeInitialData(cache1);

      WritingThread writerThread = new WritingThread(cache1, tx);
      writerThread.start();

      manager3.stop();

      // Pause for view to update
      TestingUtil.blockUntilViewsReceived(60000, false, cache1);

      cache3 = createCacheManager().getCache(cacheName);

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache3);

      writerThread.stopThread();
      writerThread.join(60000);

      verifyInitialData(cache3);

      int count = writerThread.result();

      // Since this is async, sleep a bit to allow any ongoing repls to go through
      TestingUtil.sleepThread(5000);

      for (int c = 0; c < count; c++) {
         Object o = cache3.get("test" + c);
         // Nothing should be left after a put/remove on a key
         assert o == null;
      }
   }


   private static class WritingThread extends Thread {
      private final Cache<Object, Object> cache;
      private final boolean tx;
      private volatile boolean stop;
      private volatile int result;
      private TransactionManager tm;

      WritingThread(Cache<Object, Object> cache, boolean tx) {
         super("WriterThread");
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
            try {
               if (tx) tm.begin();
               cache.put("test" + c, new PojoValue(c));
               cache.remove("test" + c);
               if (tx) tm.commit();
               c++;
               if (c % 1000 == 0) TestingUtil.sleepThread(1); // Slow it down a bit
            }
            catch (Exception e) {
               stopThread();
            }
         }
         result = c;
      }

      public void stopThread() {
         stop = true;
      }
   }

   public static class PojoValue implements Externalizable {
      Log log = LogFactory.getLog(PojoValue.class);
      static AtomicBoolean holdUp = new AtomicBoolean();
      volatile int value;

      public PojoValue() {
      }

      public PojoValue(int value) {
         this.value = value;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         String threadName = Thread.currentThread().getName();
         if (!holdUp.get() && threadName.contains("STREAMING_STATE_TRANSFER-sender")) {
            log.debug("In streaming...");
            holdUp.compareAndSet(false, true);
            log.debug("Holding up...");
            TestingUtil.sleepThread(2000); // Sleep for 2 seconds to hold up state transfer
         }

         out.writeInt(value);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         value = in.readInt();
      }

      @Override
      public int hashCode() {
         return value + 31;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         PojoValue pojo = (PojoValue) o;
         if (value != pojo.value) return false;
         return true;
      }
   }
   

}
