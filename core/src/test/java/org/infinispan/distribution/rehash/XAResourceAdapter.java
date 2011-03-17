package org.infinispan.distribution.rehash;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * abstract class that needs to be overridden
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class XAResourceAdapter implements XAResource {
   private static final Xid[] XIDS = new Xid[0];

   public void commit(Xid xid, boolean b) throws XAException {
      // no-op
   }

   public void end(Xid xid, int i) throws XAException {
      // no-op
   }

   public void forget(Xid xid) throws XAException {
      // no-op
   }

   public int getTransactionTimeout() throws XAException {
      return 0;
   }

   public boolean isSameRM(XAResource xaResource) throws XAException {
      return false;
   }

   public int prepare(Xid xid) throws XAException {
      return XA_OK;
   }

   public Xid[] recover(int i) throws XAException {
      return XIDS;
   }

   public void rollback(Xid xid) throws XAException {
      // no-op
   }

   public boolean setTransactionTimeout(int i) throws XAException {
      return false;
   }

   public void start(Xid xid, int i) throws XAException {
      // no-op
   }
}
