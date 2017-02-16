package org.infinispan.remoting;

import org.infinispan.commons.CacheException;

/**
 * Thrown when an RPC problem occurred on the caller.
 *
 * @author (various)
 * @since 4.0
 */
public class RpcException extends CacheException {

   private static final long serialVersionUID = 33172388691879866L;

   public RpcException() {
      super();
   }

   public RpcException(Throwable cause) {
      super(cause);
   }

   public RpcException(String msg) {
      super(msg);
   }

   public RpcException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
