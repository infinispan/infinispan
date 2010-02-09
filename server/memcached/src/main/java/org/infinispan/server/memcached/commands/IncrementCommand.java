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

import java.math.BigInteger;

import org.infinispan.Cache;
import org.infinispan.server.core.ChannelHandlerContext;
import org.infinispan.server.memcached.interceptors.TextProtocolVisitor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * IncrementCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class IncrementCommand extends NumericCommand {
   private static final Log log = LogFactory.getLog(IncrementCommand.class);

   public IncrementCommand(Cache cache, CommandType type, String key, String delta, boolean noReply) {
      super(cache, type, key, delta, noReply);
   }

   @Override
   public Object acceptVisitor(ChannelHandlerContext ctx, TextProtocolVisitor next) throws Throwable {
      return next.visitIncrement(ctx, this);
   }

   @Override
   protected BigInteger operate(BigInteger oldValue, BigInteger newValue) {
      if (log.isTraceEnabled()) log.trace("Increment {0} with {1}", oldValue, newValue);
      return oldValue.add(newValue);
   }


}
