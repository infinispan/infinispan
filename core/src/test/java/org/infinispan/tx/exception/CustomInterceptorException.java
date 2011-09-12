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
package org.infinispan.tx.exception;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.CustomInterceptorConfigTest;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "CustomInterceptorException")
public class CustomInterceptorException extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager eCm =
            TestCacheManagerFactory.createCacheManager(getDefaultClusteredConfig(Configuration.CacheMode.LOCAL));
      eCm.getCache().getAdvancedCache().addInterceptor(new CustomInterceptorConfigTest.DummyInterceptor() {
         @Override
         public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
            throw new IllegalStateException("Induce failure!");
         }
      }, 1);
      return eCm;
   }

   public void testFailure() throws Exception {
      TransactionManager transactionManager = cache.getAdvancedCache().getTransactionManager();
      transactionManager.begin();
      try {
         cache.put("k", "v");
         assert false;
      } catch (Exception e) {
         e.printStackTrace();
         assertEquals(transactionManager.getTransaction().getStatus(), Status.STATUS_MARKED_ROLLBACK);
      }
   }
}
