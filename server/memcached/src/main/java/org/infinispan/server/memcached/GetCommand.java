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

import org.infinispan.Cache;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;
import static org.jboss.netty.buffer.ChannelBuffers.*;
import static org.infinispan.server.memcached.RetrievalReply.VALUE;
import static org.infinispan.server.memcached.RetrievalReply.END;

/**
 * GetCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class GetCommand extends RetrievalCommand {

   GetCommand(Cache cache, CommandType type, RetrievalParameters params) {
      super(cache, type, params);
   }

   
   @Override
   public Object perform(Channel ch) throws Exception {
      ChannelBuffer buffer;
      for (String key : params.keys) {
         Value value = (Value) cache.get(key);
         if (value != null) {
            StringBuilder sb = constructValue(key, value);
            buffer = wrappedBuffer(wrappedBuffer(sb.toString().getBytes()), wrappedBuffer(CRLF),
                     wrappedBuffer(value.getData()), wrappedBuffer(CRLF));
            ch.write(buffer);
         }
      }
      
      ch.write(wrappedBuffer(wrappedBuffer(END.toString().getBytes()), wrappedBuffer(CRLF)));
      return null;
   }

   protected StringBuilder constructValue(String key, Value value) {
      StringBuilder sb = new StringBuilder();
      sb.append(VALUE).append(" ")
         .append(key).append(" ")
         .append(value.getFlags()).append(" ")
         .append(value.getData().length).append(" ");
      return sb;
   }
}
