/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.core.transport.netty;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;

/**
 * NettyChannelPipelineFactory.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NettyChannelPipelineFactory implements ChannelPipelineFactory {
   private final ChannelUpstreamHandler decoder;
   private final ChannelDownstreamHandler encoder;
   private final ChannelHandler handler;

   public NettyChannelPipelineFactory(ChannelUpstreamHandler decoder, ChannelDownstreamHandler encoder, ChannelHandler handler) {
      this.decoder = decoder;
      this.handler = handler;
      this.encoder = encoder;
   }

   @Override
   public ChannelPipeline getPipeline() throws Exception {
      // Create a default pipeline implementation.
      ChannelPipeline pipeline = pipeline();
      if (decoder != null) {
         pipeline.addLast("decoder", decoder);
      }
      if (encoder != null) {
         pipeline.addLast("encoder", encoder);
      }
      if (handler != null) {
         pipeline.addLast("handler", handler);
      }

      return pipeline;
   }

}
