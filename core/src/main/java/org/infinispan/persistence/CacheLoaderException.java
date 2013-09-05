package org.infinispan.persistence;

import org.infinispan.commons.CacheException;
import org.infinispan.persistence.spi.CacheLoader;

/**
 * An exception thrown by a {@link CacheLoader} implementation if there are problems reading from a loader.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class CacheLoaderException extends CacheException {

   private static final long serialVersionUID = -7640401612614646818L;

   public CacheLoaderException() {
   }

   public CacheLoaderException(String message) {
      super(message);
   }

   public CacheLoaderException(String message, Throwable cause) {
      super(message, cause);
   }

   public CacheLoaderException(Throwable cause) {
      super(cause);
   }
}
