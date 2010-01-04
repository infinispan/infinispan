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
 * DecrementCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class DecrementCommand extends NumericCommand {
   private static final Log log = LogFactory.getLog(DecrementCommand.class);

   public DecrementCommand(Cache cache, CommandType type, String key, BigInteger value, boolean noReply) {
      super(cache, type, key, value, noReply);
   }

   @Override
   public Object acceptVisitor(ChannelHandlerContext ctx, TextProtocolVisitor next) throws Throwable {
      return next.visitDecrement(ctx, this);
   }

   @Override
   protected BigInteger operate(BigInteger oldValue, BigInteger newValue) {
      if (log.isTraceEnabled()) log.trace("Substract {0} to {1}", newValue, oldValue);
      BigInteger b = oldValue.subtract(newValue);
      if (b.signum() < 0)
         return BigInteger.valueOf(0);
      else
         return b; 
   }

}
