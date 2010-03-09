/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc. and/or its affiliates, and
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

import org.infinispan.server.core.transport.ChannelBuffer;
import org.infinispan.server.core.transport.Encoder;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@ChannelHandler.Sharable
public class NettyEncoder extends OneToOneEncoder {
   final Encoder encoder;

   public NettyEncoder(Encoder encoder) {
      this.encoder = encoder;
   }

   @Override
   protected Object encode(org.jboss.netty.channel.ChannelHandlerContext nCtx, org.jboss.netty.channel.Channel ch,
                           Object msg) throws Exception {
      Object ret = encoder.encode(new NettyChannelHandlerContext(nCtx), new NettyChannel(ch), msg);
      if (ret instanceof ChannelBuffer) {
         ret = ((ChannelBuffer) ret).getUnderlyingChannelBuffer();
      }
      return ret;
   }
}
