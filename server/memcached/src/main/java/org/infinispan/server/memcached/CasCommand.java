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

import org.infinispan.Cache;

/**
 * CasCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class CasCommand extends SetCommand {
   final long cas;

   CasCommand(Cache cache, StorageParameters params, long cas, byte[] data) {
      super(cache, CommandType.CAS, params, data);
      this.cas = cas;
   }

   @Override
   public Command setData(byte[] data) throws IOException {
      return newCasCommand(cache, params, cas, data);
   }

   @Override
   protected StorageReply put(String key, int flags, byte[] data, long expiry) {
      Value old = (Value) cache.get(key);
      if (old != null) {
         if (old.getCas() == cas) {
            Value value = new Value(flags, data);
            boolean replaced = cache.replace(key, old, value);
            if (replaced)
               return StorageReply.STORED;
            else
               return StorageReply.EXISTS;
         } else {
            return StorageReply.EXISTS;
         }
      }
      return StorageReply.NOT_FOUND;
   }

   public static CasCommand newCasCommand(Cache cache, StorageParameters params, long cas, byte[] data) {
      return new CasCommand(cache, params, cas, data);
   }
}
