package org.infinispan.server.hotrod.transport;

import org.infinispan.server.core.transport.NettyInitializer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.FixedLengthFrameDecoder;

/**
 * A channel pipeline factory for testing that will inject a fixed length frame encoder of 2 bytes before a channel
 * handler named <b>decoder</b>
 *
 * @author William Burns
 * @since 9.0
 */
public class SingleByteFrameDecoderChannelInitializer implements NettyInitializer {
   @Override
   public void initializeChannel(Channel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addBefore("decoder", "twoframe", new FixedLengthFrameDecoder(1));
   }
}