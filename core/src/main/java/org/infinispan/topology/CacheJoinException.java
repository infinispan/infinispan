package org.infinispan.topology;

import org.infinispan.commons.CacheException;

/**
 * Thrown when a cache fails to join a cluster
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class CacheJoinException extends CacheException {
   private static final long serialVersionUID = 4394453405294292800L;

   public CacheJoinException() {
      super();
   }

   public CacheJoinException(Throwable cause) {
      super(cause);
   }

   public CacheJoinException(String msg) {
      super(msg);
   }

   public CacheJoinException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
