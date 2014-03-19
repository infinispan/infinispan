package org.infinispan.api.tree;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.impl.TreeStructureSupport;
import org.infinispan.util.concurrent.locks.LockManager;

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
      assert !TreeStructureSupport.isLocked(lm, A);
      assert !TreeStructureSupport.isLocked(lm, Fqn.ROOT);
      assert TreeStructureSupport.isLocked(lm, C);
      assert TreeStructureSupport.isLocked(lm, A_B);
      assert TreeStructureSupport.isLocked(lm, A_B_C);
   }

   protected void checkLocksDeep() {
      Cache<Object, Object> cache = cacheTL.get();
      LockManager lm = TestingUtil.extractLockManager(cache);
      assert !TreeStructureSupport.isLocked(lm, A);
      assert !TreeStructureSupport.isLocked(lm, Fqn.ROOT);
      assert !TreeStructureSupport.isLocked(lm, A_B_D);

      assert TreeStructureSupport.isLocked(lm, C);
      assert TreeStructureSupport.isLocked(lm, C_E);
      assert TreeStructureSupport.isLocked(lm, A_B);
      assert TreeStructureSupport.isLocked(lm, A_B_C);
      assert TreeStructureSupport.isLocked(lm, A_B_C_E);
   }

   protected void assertNoLocks() {
      Cache<Object, Object> cache = cacheTL.get();
      LockManager lm = TestingUtil.extractLockManager(cache);
      for (Object key : cache.keySet()) assert !lm.isLocked(key);
   }
}
