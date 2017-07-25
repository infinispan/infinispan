package org.infinispan.statetransfer;

import org.infinispan.commons.CacheException;

/**
 * Signals that all owners of a key have been lost.
 */
public class AllOwnersLostException extends CacheException {
   public static final AllOwnersLostException INSTANCE = new AllOwnersLostException();

   private AllOwnersLostException() {
      super(null, null, false, false);
   }
}
