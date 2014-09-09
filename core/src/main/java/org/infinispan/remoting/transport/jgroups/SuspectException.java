package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;

/**
 * Thrown when a member is suspected during remote method invocation
 *
 * @author Bela Ban
 * @author Galder Zamarreño
 * @since 4.0
 */
public class SuspectException extends CacheException {

   private static final long serialVersionUID = -2965599037371850141L;
   private final Address suspect;

   public SuspectException() {
      super();
      this.suspect = null;
   }

   public SuspectException(String msg) {
      super(msg);
      this.suspect = null;
   }

   public SuspectException(String msg, Address suspect) {
      super(msg);
      this.suspect = suspect;
   }

   public SuspectException(String msg, Throwable cause) {
      super(msg, cause);
      this.suspect = null;
   }

   public SuspectException(String msg, Address suspect, Throwable cause) {
      super(msg, cause);
      this.suspect = suspect;
   }

   public Address getSuspect() {
      return suspect;
   }

   public static boolean isSuspectExceptionInChain(Throwable t) {
      Throwable innerThrowable = t;
      do {
         if (innerThrowable instanceof SuspectException) {
            return true;
         }
      } while ((innerThrowable = innerThrowable.getCause()) != null);
      return false;
   }

}
