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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * CommandFactory.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class CommandFactory {
   private static final Log log = LogFactory.getLog(CommandFactory.class);

   private final Cache cache;
   private final BlockingQueue<DeleteDelayedEntry> queue;
   
   public CommandFactory(Cache cache, BlockingQueue<DeleteDelayedEntry> queue) {
      this.cache = cache;
      this.queue = queue;
   }

   public Command createCommand(String line) throws IOException {
      if (log.isTraceEnabled()) log.trace("Command line: " + line);
      String[] args = line.trim().split(" +");

      CommandType type = null;
      String tmp = args[0];
      if(tmp == null) 
         throw new EOFException();
      else
         type = CommandType.parseType(tmp);

      switch(type) {
         case SET:
         case ADD:
         case REPLACE:
         case APPEND:
         case PREPEND:
            return StorageCommand.newStorageCommand(cache, type, getStorageParameters(args), null);
         case CAS:
            tmp = args[5]; // cas unique, 64-bit integer
            long cas = Long.parseLong(tmp);
            return CasCommand.newCasCommand(cache, getStorageParameters(args), cas, null);
         case GET:
         case GETS:
            List<String> keys = new ArrayList<String>(5);
            keys.addAll(Arrays.asList(args).subList(1, args.length));
            return RetrievalCommand.newRetrievalCommand(cache, type, new RetrievalParameters(keys));
         case DELETE:
            String key = getKey(args[1]);
            long time = getOptionalTime(args[2]);
            return DeleteCommand.newDeleteCommand(cache, key, time, queue);
         default:
            return null;
      }
   }

   private StorageParameters getStorageParameters(String[] args) throws IOException {
      return new StorageParameters(getKey(args[1]), getFlags(args[2]), getExpiry(args[3]), getBytes(args[4]));
   }

   private String getKey(String key) throws IOException {
      if (key == null) throw new EOFException();
      return key;
   }

   private int getFlags(String flags) throws IOException {
      if (flags == null) throw new EOFException();
      return Integer.parseInt(flags);
   }

   private long getExpiry(String expiry) throws IOException {
      if (expiry == null) throw new EOFException();
      return Long.parseLong(expiry); // seconds
   }

   private int getBytes(String bytes) throws IOException {
      if (bytes == null) throw new EOFException();
      return Integer.parseInt(bytes);
   }

   private long getOptionalTime(String time) {
      if (time == null) return 0;
      return Long.parseLong(time); // seconds
   }
}
