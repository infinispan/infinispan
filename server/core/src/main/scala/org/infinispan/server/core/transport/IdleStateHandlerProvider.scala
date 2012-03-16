/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
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
package org.infinispan.server.core.transport

import org.jboss.netty.handler.timeout.{IdleStateEvent, IdleStateAwareChannelHandler}
import org.jboss.netty.channel.ChannelHandlerContext

/**
 * A Netty channel handler that allows idle channels to be closed.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class IdleStateHandlerProvider extends IdleStateAwareChannelHandler {

   override def channelIdle(nCtx: ChannelHandlerContext, e: IdleStateEvent) {
      nCtx.getChannel.disconnect
      super.channelIdle(nCtx, e)
   }

}