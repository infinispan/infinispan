package org.infinispan.server.core.transport;

import io.netty.channel.Channel;

/**
 * @author wburns
 * @since 9.0
 */
public interface NettyInitializer {
   /**
    * Initialize netty channel
    * @param ch
    * @throws Exception
    */
   void initializeChannel(Channel ch) throws Exception;
}
