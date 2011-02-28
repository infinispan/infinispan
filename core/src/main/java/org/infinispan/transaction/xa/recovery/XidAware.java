package org.infinispan.transaction.xa.recovery;

import javax.transaction.xa.Xid;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public interface XidAware {

   Xid getXid();

   void setXid(Xid xid);
}
