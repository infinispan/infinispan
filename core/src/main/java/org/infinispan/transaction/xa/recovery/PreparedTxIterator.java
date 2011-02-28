package org.infinispan.transaction.xa.recovery;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
*  Default implementation for RecoveryIterator.
*
* @author Mircea.Markus@jboss.com
* @since 5.0
*/
public class PreparedTxIterator implements RecoveryManager.RecoveryIterator {

   private final List<Xid[]> xids = new ArrayList<Xid[]>();

   @Override
   public boolean hasNext() {
      return !xids.isEmpty();
   }

   @Override
   public Xid[] next() {
      return xids.remove(0);
   }

   public void add(List<Xid> xids) {
      this.xids.add(xids.toArray(new Xid[xids.size()]));
   }

   @Override
   public Xid[] all() {
      List<Xid> result = new ArrayList<Xid>();
      while (hasNext()) {
         result.addAll(Arrays.asList(next()));
      }
      return result.toArray(new Xid[result.size()]);
   }

   @Override
   public void remove() {
      throw new RuntimeException("Unsupported operation!");
   }
}
