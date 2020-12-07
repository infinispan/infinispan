package org.infinispan.server.core.transport;

import java.util.concurrent.CompletionStage;

import org.infinispan.util.concurrent.NonBlockingManagerImpl;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.ThreadExecutorMap;

public class NettyNonBlockingManager extends NonBlockingManagerImpl {
   @Override
   public <V> CompletionStage<V> resumeOnSameExecutor(CompletionStage<V> stage) {
      EventExecutor thisExecutor = ThreadExecutorMap.currentExecutor();
      if (thisExecutor != null) {
         return stage.whenCompleteAsync((v, t) -> {}, thisExecutor);
      }
      return stage;
   }
}
