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
package org.infinispan.api.mvcc;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionTable;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadTest")
@CleanupAfterMethod
public class PutForExternalReadTest extends MultipleCacheManagersTest {
   final String key = "k", value = "v", value2 = "v2";

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      createClusteredCaches(2, "replSync", c);
   }

   public void testNoOpWhenKeyPresent() {
      final Cache cache1 = cache(0, "replSync");
      final Cache cache2 = cache(1, "replSync");
      cache1.putForExternalRead(key, value);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(key)) && value.equals(cache2.get(key));
         }
      });

      // reset
      cache1.remove(key);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache1.isEmpty() && cache2.isEmpty();
         }
      });

      cache1.put(key, value);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(key)) && value.equals(cache2.get(key));
         }
      });

      // now this pfer should be a no-op
      cache1.putForExternalRead(key, value2);

      assertEquals("PFER should have been a no-op", value, cache1.get(key));
      assertEquals("PFER should have been a no-op", value, cache2.get(key));
   }

   private List<Address> anyAddresses() {
      anyObject();
      return null;
   }

   private ResponseMode anyResponseMode() {
      anyObject();
      return null;
   }

   public void testTxSuspension() throws Exception {
      final Cache cache1 = cache(0, "replSync");
      final Cache cache2 = cache(1, "replSync");

      cache1.put(key + "0", value);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache2.get(key+"0"));
         }
      });

      // start a tx and do some stuff.
      tm(0, "replSync").begin();
      cache1.get(key + "0");
      cache1.putForExternalRead(key, value); // should have happened in a separate tx and have committed already.
      Transaction t = tm(0, "replSync").suspend();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(key)) && value.equals(cache2.get(key));
         }
      });

      tm(0, "replSync").resume(t);
      tm(0, "replSync").commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(key + "0")) && value.equals(cache2.get(key + "0"));
         }
      });
   }


   public void testExceptionSuppression() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      Transport mockTransport = mock(Transport.class);
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = TestingUtil.extractComponent(cache1, Transport.class);
      try {

         Address mockAddress1 = mock(Address.class);
         Address mockAddress2 = mock(Address.class);

         List<Address> memberList = new ArrayList<Address>(2);
         memberList.add(mockAddress1);
         memberList.add(mockAddress2);

         rpcManager.setTransport(mockTransport);

         when(mockTransport.getMembers()).thenReturn(memberList);

         when(mockTransport.getViewId()).thenReturn(originalTransport.getViewId());

         when(mockTransport.invokeRemotely(anyAddresses(), (CacheRpcCommand) anyObject(), anyResponseMode(),
                                             anyLong(), anyBoolean(), (ResponseFilter) anyObject()))
               .thenThrow(new RuntimeException("Barf!"));

         try {
            cache1.put(key, value);
            fail("Should have barfed");
         }
         catch (RuntimeException re) {
         }

         // clean up any indeterminate state left over
         try {
            cache1.remove(key);
            fail("Should have barfed");
         }
         catch (RuntimeException re) {
         }

         assertNull("Should have cleaned up", cache1.get(key));

         // should not barf
         cache1.putForExternalRead(key, value);
      }
      finally {
         if (rpcManager != null) rpcManager.setTransport(originalTransport);
      }
   }

   public void testBasicPropagation() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");

      assert !cache1.containsKey(key);
      assert !cache2.containsKey(key);
      ReplListener replListener2 = replListener(cache2);

      replListener2.expect(PutKeyValueCommand.class);
      cache1.putForExternalRead(key, value);
      replListener2.waitForRpc();

      assertEquals("PFER updated cache1", value, cache1.get(key));
      assertEquals("PFER propagated to cache2 as expected", value, cache2.get(key));

      // replication to cache 1 should NOT happen.
      cache2.putForExternalRead(key, value + "0");

      assertEquals("PFER updated cache2", value, cache2.get(key));
      assertEquals("Cache1 should be unaffected", value, cache1.get(key));
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead().
    *
    * @throws Exception
    */
   public void testSimpleCacheModeLocal(Method m) throws Exception {
      cacheModeLocalTest(false, m);
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead() when the call
    * occurs inside a transaction.
    *
    * @throws Exception
    */
   public void testCacheModeLocalInTx(Method m) throws Exception {
      cacheModeLocalTest(true, m);
   }

   /**
    * Tests that suspended transactions do not leak.  See JBCACHE-1246.
    */
   public void testMemLeakOnSuspendedTransactions() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      TransactionManager tm1 = TestingUtil.getTransactionManager(cache1);
      TransactionManager tm2 = TestingUtil.getTransactionManager(cache2);
      ReplListener replListener2 = replListener(cache2);

      replListener2.expect(PutKeyValueCommand.class);
      tm1.begin();
      cache1.putForExternalRead(key, value);
      tm1.commit();
      replListener2.waitForRpc();

      final TransactionTable tt1 = TestingUtil.extractComponent(cache1, TransactionTable.class);
      final TransactionTable tt2 = TestingUtil.extractComponent(cache2, TransactionTable.class);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return tt1.getRemoteTxCount() == 0 && tt1.getLocalTxCount() == 0 &&
                  tt2.getRemoteTxCount() == 0 && tt2.getLocalTxCount() == 0;
         }
      });

      replListener2.expectWithTx(PutKeyValueCommand.class);
      tm1.begin();
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      cache1.putForExternalRead(key, value);
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      cache1.put(key, value);
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      log.info("Before commit!!");
      tm1.commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
                  && (tt2.getLocalTxCount() == 0);
         }
      });

      replListener2.expectWithTx(PutKeyValueCommand.class);
      tm1.begin();
      cache1.put(key, value);
      cache1.putForExternalRead(key, value);
      tm1.commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
                  && (tt2.getLocalTxCount() == 0);
         }
      });

      replListener2.expectWithTx(PutKeyValueCommand.class, PutKeyValueCommand.class);
      tm1.begin();
      cache1.put(key, value);
      cache1.putForExternalRead(key, value);
      cache1.put(key, value);
      tm1.commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
                  && (tt2.getLocalTxCount() == 0);
         }
      });
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead().
    *
    * @throws Exception
    */
   private void cacheModeLocalTest(boolean transactional, Method m) throws Exception {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      Cache<Object, Object> cache2 = cache(1, "replSync");
      TransactionManager tm1 = TestingUtil.getTransactionManager(cache1);
      if (transactional)
         tm1.begin();

      String k = k(m);
      cache1.getAdvancedCache().withFlags(CACHE_MODE_LOCAL).putForExternalRead(k, v(m));
      assertFalse(cache2.containsKey(k));

      if (transactional)
         tm1.commit();
   }
}
