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

import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import java.io.Serializable;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.ExceptionInCommandTest")
public class ExceptionInCommandTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true), 2);
      waitForClusterToForm();
   }                                                                        

   public void testPutThrowsLocalException() throws Exception {
      tm(0).begin();

      Delta d = new Delta() {
         public DeltaAware merge(DeltaAware d) {
            throw new RuntimeException("Induced!");
         }
      };

      try {
         cache(0).put("k", d);
         assert false;
      } catch (RuntimeException e) {
         assert tx(0).getStatus() == Status.STATUS_MARKED_ROLLBACK;
      }
   }

   @Test (expectedExceptions = RollbackException.class)
   public void testPutThrowsRemoteException() throws Exception {
      tm(0).begin();

      MyDelta d = new MyDelta();
      d.setCreator();

      cache(0).put("k", d);

      tm(0).commit();

   }

   private static class MyDelta implements Delta , Serializable {
      transient Thread creator;

      public void setCreator() {creator = Thread.currentThread();}

      public DeltaAware merge(DeltaAware d) {
         if (creator != Thread.currentThread())
            throw new RuntimeException("Induced!");
         return new AtomicHashMap();
      }
   }
}
