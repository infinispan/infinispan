package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodClientPipelaneFactory implements ChannelPipelineFactory {

   private static Log log = LogFactory.getLog(HotRodClientPipelaneFactory.class);

   private HotRodClientDecoder decoder;

   public HotRodClientPipelaneFactory(HotRodClientDecoder decoder) {
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
      pipeline.addLast("encoder", new HotRodClientEncoder());
      return pipeline;
   }
}
