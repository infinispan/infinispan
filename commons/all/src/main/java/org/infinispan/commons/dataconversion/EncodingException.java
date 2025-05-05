package org.infinispan.commons.dataconversion;

import org.infinispan.commons.CacheException;

/**
 * Exception thrown when an error occurs during data encoding.
 *
 * @since 9.1
 */
public class EncodingException extends CacheException {
   public EncodingException(String message) {
      super(message);
   }

   public EncodingException(String message, Throwable cause) {
      super(message, cause);
   }
}
