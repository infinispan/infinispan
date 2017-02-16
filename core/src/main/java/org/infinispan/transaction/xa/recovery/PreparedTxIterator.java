package org.infinispan.transaction.xa.recovery;

import java.util.HashSet;
import java.util.List;

import javax.transaction.xa.Xid;

/**
*  Default implementation for RecoveryIterator.
*
* @author Mircea.Markus@jboss.com
* @since 5.0
*/
public class PreparedTxIterator implements RecoveryManager.RecoveryIterator {

   private final HashSet<Xid> xids = new HashSet<Xid>(4);

   @Override
   public boolean hasNext() {
      return !xids.isEmpty();
   }

   @Override
   public Xid[] next() {
      Xid[] result = xids.toArray(new Xid[xids.size()]);
      xids.clear();
      return result;
   }

   public void add(List<Xid> xids) {
      this.xids.addAll(xids);
   }

   @Override
   public Xid[] all() {
      return next();
   }

   @Override
   public void remove() {
      throw new RuntimeException("Unsupported operation!");
   }
}
