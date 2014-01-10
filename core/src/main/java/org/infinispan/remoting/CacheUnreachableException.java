package org.infinispan.remoting;

import org.jgroups.UnreachableException;

/**
 * Wraps the UnreachableException.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class CacheUnreachableException extends RuntimeException {

   public CacheUnreachableException(UnreachableException e) {
      super(e.toString());
   }

}
