package org.infinispan.commons.marshall;

import org.infinispan.commons.CacheException;

/**
 * An exception that hides inner stacktrace lines for non serializable exceptions.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NotSerializableException extends CacheException {

   private static final long serialVersionUID = 8217398736102723887L;

   public NotSerializableException(String message, Throwable cause) {
      super(message, cause);
   }

   public NotSerializableException(String message) {
      super(message);
   }

   @Override
   public void setStackTrace(StackTraceElement[] stackTrace) {
      // nothing
   }

   @Override
   public Throwable fillInStackTrace() {
      // no operation
      return this;
   }

}
