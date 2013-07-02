package org.infinispan.transaction.xa.recovery;

import javax.transaction.xa.Xid;

/**
 * Interface that adds recovery required information to a {@link org.infinispan.transaction.xa.GlobalTransaction}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public interface RecoverableTransactionIdentifier {

   Xid getXid();

   void setXid(Xid xid);

   long getInternalId();

   void setInternalId(long internalId);
}
