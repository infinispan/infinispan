package org.infinispan;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Deprecated
public class CacheException extends org.infinispan.commons.CacheException {

   private static final long serialVersionUID = -4386393072593859164L;

   public CacheException() {
      super();
   }

   public CacheException(Throwable cause) {
      super(cause);
   }

   public CacheException(String msg) {
      super(msg);
   }

   public CacheException(String msg, Throwable cause) {
      super(msg, cause);
   }

}
