package org.infinispan.util.concurrent;

import org.infinispan.commons.CacheException;


/**
 * Thrown when a timeout occurred. used by operations with timeouts, e.g. lock acquisition, or waiting for responses
 * from all members.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a>.
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TimeoutException extends CacheException {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -8096787619908687038L;

   public TimeoutException() {
      super();
   }

   public TimeoutException(String msg) {
      super(msg);
   }

   public TimeoutException(String msg, Throwable cause) {
      super(msg, cause);
   }

   @Override
   public String toString() {
      return super.toString();
   }
}
