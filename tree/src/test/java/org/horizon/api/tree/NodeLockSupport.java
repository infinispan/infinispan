/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
 *
 */

package org.horizon.api.tree;

import org.horizon.Cache;
import org.horizon.test.TestingUtil;
import org.horizon.lock.LockManager;
import org.horizon.tree.Fqn;
import org.horizon.tree.TreeCache;
import org.horizon.tree.TreeStructureSupport;

import javax.transaction.TransactionManager;

public abstract class NodeLockSupport {
   static final Fqn A = Fqn.fromString("/a"), B = Fqn.fromString("/b"), C = Fqn.fromString("/c"), D = Fqn.fromString("/d"), E = Fqn.fromString("/e");
   static final Object k = "key", vA = "valueA", vB = "valueB", vC = "valueC", vD = "valueD", vE = "valueE";
   static final Fqn A_B = Fqn.fromRelativeFqn(A, B);
   static final Fqn A_B_C = Fqn.fromRelativeFqn(A_B, C);
   static final Fqn A_B_C_E = Fqn.fromRelativeFqn(A_B_C, E);
   static final Fqn A_B_D = Fqn.fromRelativeFqn(A_B, D);
   static final Fqn C_E = Fqn.fromRelativeFqn(C, E);
   static final Fqn D_B = Fqn.fromRelativeFqn(D, B);
   static final Fqn D_B_C = Fqn.fromRelativeFqn(D_B, C);

   protected ThreadLocal<Cache<Object, Object>> cacheTL = new ThreadLocal<Cache<Object, Object>>();
   protected ThreadLocal<TransactionManager> tmTL = new ThreadLocal<TransactionManager>();
   protected ThreadLocal<TreeCache> treeCacheTL = new ThreadLocal<TreeCache>();

   protected void checkLocks() {
      Cache<Object, Object> cache = cacheTL.get();
      LockManager lm = TestingUtil.extractLockManager(cache);
      assert !TreeStructureSupport.isLocked(cache, lm, A);
      assert !TreeStructureSupport.isLocked(cache, lm, Fqn.ROOT);
      assert TreeStructureSupport.isLocked(cache, lm, C);
      assert TreeStructureSupport.isLocked(cache, lm, A_B);
      assert TreeStructureSupport.isLocked(cache, lm, A_B_C);
   }

   protected void checkLocksDeep() {
      Cache<Object, Object> cache = cacheTL.get();
      LockManager lm = TestingUtil.extractLockManager(cache);
      assert !TreeStructureSupport.isLocked(cache, lm, A);
      assert !TreeStructureSupport.isLocked(cache, lm, Fqn.ROOT);
      assert !TreeStructureSupport.isLocked(cache, lm, A_B_D);

      assert TreeStructureSupport.isLocked(cache, lm, C);
      assert TreeStructureSupport.isLocked(cache, lm, C_E);
      assert TreeStructureSupport.isLocked(cache, lm, A_B);
      assert TreeStructureSupport.isLocked(cache, lm, A_B_C);
      assert TreeStructureSupport.isLocked(cache, lm, A_B_C_E);
   }

   protected void assertNoLocks() {
      Cache<Object, Object> cache = cacheTL.get();
      LockManager lm = TestingUtil.extractLockManager(cache);
      for (Object key : cache.keySet()) assert !lm.isLocked(key);
   }
}
