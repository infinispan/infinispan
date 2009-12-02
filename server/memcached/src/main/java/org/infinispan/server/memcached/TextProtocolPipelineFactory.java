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
package org.infinispan.server.memcached;

import static org.jboss.netty.channel.Channels.*;

import org.infinispan.Cache;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

/**
 * TextProtocolPipelineFactory.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
class TextProtocolPipelineFactory implements ChannelPipelineFactory {

//   private final ChannelHandler handler;
//
//   public TextProtocolPipelineFactory(TextCommandHandler handler) {
//      this.handler = handler;
//   }

   private final Cache cache;

   public TextProtocolPipelineFactory(Cache cache) {
      this.cache = cache;
   }

   @Override
   public ChannelPipeline getPipeline() throws Exception {
      // Create a default pipeline implementation.
      ChannelPipeline pipeline = pipeline();
      pipeline.addLast("decoder", new TextCommandDecoder(cache));
      pipeline.addLast("handler", new TextCommandHandler());
//      pipeline.addLast("encoder", new TextResponseEncoder());

      return pipeline;
   }

}
