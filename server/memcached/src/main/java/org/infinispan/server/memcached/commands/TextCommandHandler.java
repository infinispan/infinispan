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
package org.infinispan.server.memcached.commands;

import org.infinispan.Cache;
import org.infinispan.server.core.ChannelHandlerContext;
import org.infinispan.server.core.CommandHandler;
import org.infinispan.server.core.MessageEvent;
import org.infinispan.server.core.InterceptorChain;

/**
 * TextProtocolServerHandler.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class TextCommandHandler implements CommandHandler {
   final Cache cache;
   final InterceptorChain chain;

   public TextCommandHandler(Cache cache, InterceptorChain chain) {
      this.cache = cache;
      this.chain = chain;
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Throwable {
      chain.invoke(ctx, (TextCommand) e.getMessage());
   }

}
