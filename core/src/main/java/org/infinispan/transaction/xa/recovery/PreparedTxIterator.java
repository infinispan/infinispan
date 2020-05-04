package org.infinispan.transaction.xa.recovery;

import java.util.HashSet;
import java.util.List;

import org.infinispan.commons.tx.XidImpl;

/**
*  Default implementation for RecoveryIterator.
*
* @author Mircea.Markus@jboss.com
* @since 5.0
*/
public class PreparedTxIterator implements RecoveryManager.RecoveryIterator {

   private final HashSet<XidImpl> xids = new HashSet<>(4);

   @Override
   public boolean hasNext() {
      return !xids.isEmpty();
   }

   @Override
   public XidImpl[] next() {
      XidImpl[] result = xids.toArray(new XidImpl[0]);
      xids.clear();
      return result;
   }

   public void add(List<XidImpl> xids) {
      this.xids.addAll(xids);
   }

   @Override
   public XidImpl[] all() {
      return next();
   }

   @Override
   public void remove() {
      throw new RuntimeException("Unsupported operation!");
   }
}
