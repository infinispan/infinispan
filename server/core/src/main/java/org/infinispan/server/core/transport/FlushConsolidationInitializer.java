package org.infinispan.server.core.transport;

import io.netty.channel.Channel;
import io.netty.handler.flush.FlushConsolidationHandler;

public class FlushConsolidationInitializer implements NettyInitializer {
   @Override
   public void initializeChannel(Channel ch) throws Exception {
      ch.pipeline().addFirst("flush-consolidation", new FlushConsolidationHandler());
   }
}
