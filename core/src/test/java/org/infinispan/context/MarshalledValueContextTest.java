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
package org.infinispan.context;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.Serializable;

import static org.testng.Assert.assertEquals;

/**
 * This is to test that contexts are properly constructed and cleaned up wven when using marshalled values and the
 * explicit lock() API.
 *
 * @author Manik Surtani
 * @version 4.1
 */
@Test (testName = "context.MarshalledValueContextTest", groups = "functional")
public class MarshalledValueContextTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = TestCacheManagerFactory.getDefaultConfiguration(true)
            .fluent()
               .storeAsBinary()
               .transaction()
                  .lockingMode(LockingMode.PESSIMISTIC)
            .build();
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testContentsOfContext() throws Exception {
      Cache<Key, String> c = cacheManager.getCache();
      ContextExtractingInterceptor cex = new ContextExtractingInterceptor();
      c.getAdvancedCache().addInterceptorAfter(cex, InvocationContextInterceptor.class);

      c.put(new Key("k"), "v");

      assert "v".equals(c.get(new Key("k")));

      TransactionManager tm = c.getAdvancedCache().getTransactionManager();
      tm.begin();
      c.getAdvancedCache().lock(new Key("k"));

      LockManager lockManager = TestingUtil.extractComponent(c, LockManager.class);

      assert cex.ctx instanceof LocalTxInvocationContext;

      assert cex.ctx.getLookedUpEntries().size() == 0 : "Looked up key should not be in transactional invocation context " +
                                                      "as we don't perform any changes";
      assertEquals(lockManager.getNumberOfLocksHeld(), 1, "Only one lock should be held");

      c.put(new Key("k"), "v2");

      assert cex.ctx.getLookedUpEntries().size() == 1 : "Still should only be one entry in the context";
      assert lockManager.getNumberOfLocksHeld() == 1 : "Only one lock should be held";

      tm.commit();

      assert lockManager.getNumberOfLocksHeld() == 0 : "No locks should be held anymore";

      assert "v2".equals(c.get(new Key("k")));
   }

   private static class ContextExtractingInterceptor extends CommandInterceptor {
      InvocationContext ctx;
      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         this.ctx = ctx;
         return super.invokeNextInterceptor(ctx, command);
      }
   }

   private static class Key implements Serializable {
      String actualKey;

      private Key(String actualKey) {
         this.actualKey = actualKey;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Key key = (Key) o;

         if (actualKey != null ? !actualKey.equals(key.actualKey) : key.actualKey != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return actualKey != null ? actualKey.hashCode() : 0;
      }
   }
}

