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

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.util.NotImplementedException;

import org.infinispan.server.core.InterceptorChain;

/**
 * CommandFactory.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class CommandFactory {
   private static final Log log = LogFactory.getLog(CommandFactory.class);
   private static final String NO_REPLY = "noreply";

   private final Cache<String, Value> cache;
   private final InterceptorChain chain;
   private final ScheduledExecutorService scheduler;
   
   public CommandFactory(Cache<String, Value> cache, InterceptorChain chain, ScheduledExecutorService scheduler) {
      this.cache = cache;
      this.chain = chain;
      this.scheduler = scheduler;
   }

   public TextCommand createCommand(String line) throws IOException {
      if (log.isTraceEnabled()) log.trace("Command line: " + line);
      String[] args = line.trim().split(" +");

      CommandType type;
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
            return StorageCommand.newStorageCommand(cache, type, getStorageParameters(args), null, parseNoReply(5, args));
         case CAS:
            tmp = args[5]; // cas unique, 64-bit integer
            long cas = Long.parseLong(tmp);
            return CasCommand.newCasCommand(cache, getStorageParameters(args), cas, null, parseNoReply(6, args));
         case GET:
         case GETS:
            List<String> keys = new ArrayList<String>(5);
            keys.addAll(Arrays.asList(args).subList(1, args.length));
            return RetrievalCommand.newRetrievalCommand(cache, type, new RetrievalParameters(keys));
         case DELETE:
            String delKey = getKey(args[1]);
            int time = parseDelayedDeleteTime(2, args);
            boolean noReply = false;
            if (time == -1) {
               // Try parsing noreply just in case
               noReply = parseNoReply(2, args);
            } else {
               // 0 or positive numbers are ignored; We immediately delete since this is no longer in the spec:
               // http://github.com/trondn/memcached/blob/master/doc/protocol.txt
            }
            return DeleteCommand.newDeleteCommand(cache, delKey, noReply);
         case INCR:
         case DECR:
            String key = getKey(args[1]);
            String delta = args[2];
            return NumericCommand.newNumericCommand(cache, type, key, delta, parseNoReply(3, args));
         case STATS:
            return StatsCommand.newStatsCommand(cache, type, chain);
         case FLUSH_ALL:
            long delay = args.length > 1 ? Long.parseLong(args[1]) : 0;
            return FlushAllCommand.newFlushAllCommand(cache, delay, scheduler, parseNoReply(2, args));
         case VERSION:
            return VersionCommand.newVersionCommand();
         case QUIT:
            return QuitCommand.newQuitCommand();
         default:
            throw new NotImplementedException("Parsed type not implemented yet");
      }
   }

   private StorageParameters getStorageParameters(String[] args) throws IOException {
      return new StorageParameters(getKey(args[1]), getFlags(args[2]), getExpiry(args[3]), getBytes(args[4]));
   }

   private String getKey(String key) throws IOException {
      if (key == null) throw new EOFException("No key passed");
      return key;
   }

   private int getFlags(String flags) throws IOException {
      if (flags == null) throw new EOFException("No flags passed");
      return Integer.parseInt(flags);
   }

   private long getExpiry(String expiry) throws IOException {
      if (expiry == null) throw new EOFException("No expiry passed");
      return Long.parseLong(expiry); // seconds
   }

   private int getBytes(String bytes) throws IOException {
      if (bytes == null) throw new EOFException("No bytes for storage command passed");
      return Integer.parseInt(bytes);
   }

   private boolean parseNoReply(int expectedIndex, String[] args) throws IOException {
      if (args.length > expectedIndex) {
         if (NO_REPLY.equals(args[expectedIndex])) {
            return true;
         } else {
            throw new StreamCorruptedException("Unable to parse noreply optional argument");
         }
      }
      return false;
   }

   private int parseDelayedDeleteTime(int expectedIndex, String[] args) {
      if (args.length > expectedIndex) {
         try {
            return Integer.parseInt(args[expectedIndex]);
         } catch (NumberFormatException e) {
            // Either unformatted number, or noreply found
            return -1;
         }
      }
      return 0;
   }
}
