package org.infinispan.commons.dataconversion;

import org.infinispan.commons.CacheException;

/**
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
