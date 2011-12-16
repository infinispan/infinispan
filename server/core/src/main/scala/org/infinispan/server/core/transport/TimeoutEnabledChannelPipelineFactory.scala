/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.server.core.transport

import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.infinispan.server.core.ProtocolServer
import org.jboss.netty.channel.{ChannelDownstreamHandler, Channels, ChannelPipeline}

/**
 * A channel pipeline factory for environments where idle timeout is enabled.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
class TimeoutEnabledChannelPipelineFactory(server: ProtocolServer,
                                           encoder: ChannelDownstreamHandler,
                                           transport: NettyTransport,
                                           idleTimeout: Int)
      extends NettyChannelPipelineFactory(server, encoder, transport) {

   import TimeoutEnabledChannelPipelineFactory._

   override def getPipeline: ChannelPipeline = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", server.getDecoder)
      if (encoder != null)
         pipeline.addLast("encoder", encoder)

      pipeline.addLast("idleHandler", new IdleStateHandler(timer, idleTimeout, 0, 0))
      pipeline.addLast("idleHandlerProvider", new IdleStateHandlerProvider)
      return pipeline;
   }

   override def stop {
      timer.stop()
   }

}

object TimeoutEnabledChannelPipelineFactory {

   lazy val timer = new HashedWheelTimer

}