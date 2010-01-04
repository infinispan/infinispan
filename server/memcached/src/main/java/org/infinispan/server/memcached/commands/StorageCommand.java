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

import java.io.IOException;
import java.io.StreamCorruptedException;

import org.infinispan.Cache;
import org.infinispan.server.core.Command;

/**
 * StorageCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class StorageCommand implements TextCommand {
   private final CommandType type;
   final Cache<String, Value> cache;
   final StorageParameters params;
   final byte[] data;
   final boolean noReply;

   StorageCommand(Cache<String, Value> cache, CommandType type, StorageParameters params, byte[] data, boolean noReply) {
      this.type = type;
      this.params = params;
      this.cache = cache;
      this.data = data;
      this.noReply = noReply;
   }

   public CommandType getType() {
      return type;
   }

   public Command setData(byte[] data) throws IOException {
      return newStorageCommand(cache, type, params, data, noReply);
   }

   public StorageParameters getParams() {
      return params;
   }

   public static TextCommand newStorageCommand(Cache<String, Value> cache, CommandType type, StorageParameters params, byte[] data, boolean noReply) throws IOException {
      switch(type) {
         case SET: return new SetCommand(cache, type, params, data, noReply);
         case ADD: return new AddCommand(cache, type, params, data, noReply);
         case REPLACE: return new ReplaceCommand(cache, type, params, data, noReply);
         case APPEND: return new AppendCommand(cache, type, params, data, noReply);
         case PREPEND: return new PrependCommand(cache, type, params, data, noReply);
         default: throw new StreamCorruptedException("Unable to build storage command for type: " + type);
      }
   }
}
