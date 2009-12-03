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

import java.io.IOException;
import java.io.StreamCorruptedException;

import org.infinispan.Cache;

/**
 * StorageCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class StorageCommand implements Command {
   final Cache cache;
   private final CommandType type;
   final StorageParameters params;
//   final String key;
//   final int flags;
//   final long expiry;
//   final int bytes;
   final byte[] data;

   StorageCommand(Cache cache, CommandType type, StorageParameters params, byte[] data) {
      this.type = type;
      this.params = params;
      this.cache = cache;
//      this.key = key;
//      this.flags = flags;
//      this.expiry = expiry;
//      this.bytes = bytes;
      this.data = data;
   }

   public CommandType getType() {
      return type;
   }

   public Command setData(byte[] data) throws IOException {
      return newStorageCommand(cache, type, params, data);
   }

   public static Command newStorageCommand(Cache cache, CommandType type, StorageParameters params, byte[] data) throws IOException {
      switch(type) {
         case SET: return new SetCommand(cache, type, params, data);
         case ADD: return new AddCommand(cache, type, params, data);
         case REPLACE: return new ReplaceCommand(cache, type, params, data);
         case APPEND: return new AppendCommand(cache, type, params, data);
         case PREPEND: return new PrependCommand(cache, type, params, data);
         default: throw new StreamCorruptedException("Unable to build storage command for type: " + type);
      }
   }
   
//   public static Command buildCasCommand(String key, int flags, long expiry, int bytes, long unique) {
//      return new CasCommand(key, flags, expiry, bytes, unique);
//   }
}
