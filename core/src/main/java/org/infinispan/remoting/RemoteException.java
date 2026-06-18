package org.infinispan.remoting;

import org.infinispan.commons.CacheException;

/**
 * Represents an application-level exception originating in a remote node.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class RemoteException extends CacheException {

   public RemoteException(String msg, Throwable cause) {
      super(msg, cause);
   }

   public RemoteException(String msg, Throwable cause, boolean writableStackTrace) {
      super(msg, cause, true, writableStackTrace);
   }

}
