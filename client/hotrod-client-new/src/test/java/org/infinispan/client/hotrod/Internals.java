package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;

/**
 * This method is for internal usage only and may change without notice at any point.
 */
public class Internals {
   public static OperationDispatcher dispatcher(RemoteCacheManager cacheManager) {
      return cacheManager.getOperationDispatcher();
   }
}
