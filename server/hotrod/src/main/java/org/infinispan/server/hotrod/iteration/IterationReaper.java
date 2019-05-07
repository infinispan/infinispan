package org.infinispan.server.hotrod.iteration;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class IterationReaper implements GenericFutureListener<Future<? super Void>> {

   private final IterationManager iterationManager;
   private final String cacheName;
   private final String iterationId;

   public IterationReaper(IterationManager iterationManager, String cacheName, String iterationId) {
      this.iterationManager = iterationManager;
      this.cacheName = cacheName;
      this.iterationId = iterationId;
   }

   @Override
   public void operationComplete(Future<? super Void> future) {
      iterationManager.close(cacheName, iterationId);
   }
}
