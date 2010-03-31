package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import static org.jboss.netty.channel.Channels.*;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotrodClientPipelaneFactory implements ChannelPipelineFactory {

   private static Log log = LogFactory.getLog(HotrodClientPipelaneFactory.class);

   private HotrodClientDecoder decoder;

   public HotrodClientPipelaneFactory(HotrodClientDecoder decoder) {
      this.decoder = decoder;
   }

   @Override
   public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = new DefaultChannelPipeline() {
         @Override
         protected void notifyHandlerException(ChannelEvent e, Throwable t) {
            log.warn("Exception on event: " + e, t);
            super.notifyHandlerException(e, t);
         }
      };
      pipeline.addLast("decoder", decoder);
      pipeline.addLast("encoder", new HotrodClientEncoder());
      return pipeline;
   }
}
