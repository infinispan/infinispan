package org.infinispan.marshall.core;

import org.infinispan.commons.CacheException;

/**
 * An exception thrown by a {@link MarshalledEntry} implementation if there are marshalling/unmarshalling data.
 *
 * @author Ryan Emerson
 * @since 9.4
 */
public class MarshallingException extends CacheException {

   public MarshallingException() {
   }

   public MarshallingException(String message) {
      super(message);
   }

   public MarshallingException(String message, Throwable cause) {
      super(message, cause);
   }

   public MarshallingException(Throwable cause) {
      super(cause);
   }
}
