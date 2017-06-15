package org.infinispan.remoting;

import org.infinispan.commons.CacheException;
import org.jgroups.UnreachableException;

/**
 * Signals a backup site was unreachable.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class CacheUnreachableException extends CacheException {

   public CacheUnreachableException(String message) {
      super(message);
   }

   public CacheUnreachableException(UnreachableException e) {
      super(e.toString());
   }

}
