package org.infinispan.statetransfer;

import org.infinispan.commons.CacheException;

/**
 * Signalizes that all owners of some key have been lost.
 */
public class AllOwnersLostException extends CacheException {
   public final static AllOwnersLostException INSTANCE = new AllOwnersLostException();

   private AllOwnersLostException() {
      super(null, null, false, false);
   }
}
