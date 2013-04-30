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

package org.infinispan.lock;

import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.OwnableReentrantLock;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Test (groups = "functional", testName = "lock.SimpleLockContainerTest")
public class SimpleLockContainerTest extends AbstractInfinispanTest {

   OwnableReentrantPerEntryLockContainer lc = new OwnableReentrantPerEntryLockContainer(1000);

   public void simpleTest() throws Exception {
      final String k1 = ab();
      final String k2 = ab2();
      assert k1 != k2 && k1.equals(k2);

      Object owner = new Object();
      lc.acquireLock(owner, k1, 0, TimeUnit.MILLISECONDS);
      assert lc.isLocked(k1);


      fork(new Runnable() {
         @Override
         public void run() {
            final Object otherOwner = new Object();
            for (int i =0; i < 10; i++) {
               try {
                  final OwnableReentrantLock ownableReentrantLock = lc.acquireLock(otherOwner, k2, 500, TimeUnit.MILLISECONDS);
                  log.trace("ownableReentrantLock = " + ownableReentrantLock);
                  if (ownableReentrantLock != null) return;
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
         }
      }, false);

      Thread.sleep(200);
      lc.releaseLock(owner, k1);

      Thread.sleep(4000);
   }

   private String ab2() {
      return "ab";
   }

   public String ab() {
      StringBuilder sb = new StringBuilder("a");
      return sb.append("b").toString();
   }
}
