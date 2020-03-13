package org.infinispan.commons.marshall;

import org.infinispan.commons.CacheException;

/**
 * An exception that can be thrown by a cache if an object cannot be successfully serialized/deserialized.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class MarshallingException extends CacheException {
   public MarshallingException() {
      super();
   }

   public MarshallingException(Throwable cause) {
      super(cause);
   }

   public MarshallingException(String msg) {
      super(msg);
   }

   public MarshallingException(String msg, Throwable cause) {
      super(msg, cause);
   }

   public MarshallingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }
}
