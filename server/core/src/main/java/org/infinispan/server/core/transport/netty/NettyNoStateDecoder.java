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

import org.infinispan.server.core.transport.NoState;
import org.infinispan.server.core.transport.NoStateDecoder;
import org.infinispan.server.core.transport.ChannelBuffer;
import org.infinispan.server.core.transport.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 */
public class NettyNoStateDecoder extends ReplayingDecoder<NoState> {
   final NoStateDecoder decoder;

   public NettyNoStateDecoder(NoStateDecoder decoder) {
      super(true);
      this.decoder = decoder;
   }

   @Override
   protected Object decode(org.jboss.netty.channel.ChannelHandlerContext nCtx, org.jboss.netty.channel.Channel channel,
                           org.jboss.netty.buffer.ChannelBuffer nBuffer, NoState state) throws Exception {
      ChannelHandlerContext ctx = new NettyChannelHandlerContext(nCtx);
      ChannelBuffer buffer = new NettyChannelBuffer(nBuffer);
      return decoder.decode(ctx, buffer);
   }

   @Override
   public void exceptionCaught(org.jboss.netty.channel.ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      decoder.exceptionCaught(new NettyChannelHandlerContext(ctx), new NettyExceptionEvent(e));
   }
}
