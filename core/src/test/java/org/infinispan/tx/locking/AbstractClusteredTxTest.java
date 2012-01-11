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

package org.infinispan.tx.locking;

import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.transaction.tm.DummyXid;

import javax.transaction.xa.XAException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractClusteredTxTest extends MultipleCacheManagersTest {
   
   Object k;

   public void testPut() throws Exception {
      tm(0).begin();
      cache(0).put(k, "v");
      assertLocking();
   }

   public void testRemove() throws Exception {
      tm(0).begin();
      cache(0).remove(k);
      assertLocking();
   }

   public void testReplace() throws Exception {
      tm(0).begin();
      cache(0).replace(k, "v");
      assertLocking();
   }

   public void testClear() throws Exception {
      cache(0).put(k, "v");
      tm(0).begin();
      cache(0).clear();
      assertLocking();
   }

   public void testPutAll() throws Exception {
      Map m = Collections.singletonMap(k, "v");
      tm(0).begin();
      cache(0).putAll(m);
      assertLocking();
   }

   protected void commit() {
      DummyTransactionManager dtm = (DummyTransactionManager) tm(0);
      try {
         dtm.firstEnlistedResource().commit(new DummyXid(UUID.randomUUID()), true);
      } catch (XAException e) {
         throw new RuntimeException(e);
      }
   }

   protected void prepare() {
      DummyTransactionManager dtm = (DummyTransactionManager) tm(0);
      try {
         dtm.firstEnlistedResource().prepare(new DummyXid(UUID.randomUUID()));
      } catch (XAException e) {
         throw new RuntimeException(e);
      }
   }

   protected abstract void assertLocking();
}
