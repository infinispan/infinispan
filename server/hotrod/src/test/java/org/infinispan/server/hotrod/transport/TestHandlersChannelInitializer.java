package org.infinispan.server.hotrod.transport;

import org.infinispan.server.core.transport.NettyInitializer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.FixedLengthFrameDecoder;

/**
 * A channel pipeline factory for testing that adds 2 handlers before a channel handler named <b>decoder</b>:
 * <ul>
 *    <li>A logging decoder to log message contents at trace level</li>
 *    <li>A fixed length frame decoder of 1 byte</li>
 * </ul>
 *
 * @author William Burns
 * @since 9.0
 */
public class TestHandlersChannelInitializer implements NettyInitializer {
   @Override
   public void initializeChannel(Channel ch) {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addBefore("decoder", "trace", new TraceChannelHandler());
      // Note: FixedLengthFrameDecoder never passes a buffer < frame length, so a higher frame length will not work
      pipeline.addBefore("decoder", "1frame", new FixedLengthFrameDecoder(1));
   }
}
