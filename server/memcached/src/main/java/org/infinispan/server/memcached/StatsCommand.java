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

import static org.infinispan.server.memcached.Reply.END;
import static org.infinispan.server.memcached.Reply.STAT;
import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.jboss.netty.channel.Channel;

/**
 * StatsCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class StatsCommand implements Command {
   final Cache cache;
   private final CommandType type;
   final InterceptorChain chain;

   StatsCommand(Cache cache, CommandType type, InterceptorChain chain) {
      this.cache = cache;
      this.type = type;
      this.chain = chain;
   }

   @Override
   public CommandType getType() {
      return CommandType.STATS;
   }

   @Override
   public Object acceptVisitor(Channel ch, CommandInterceptor next) throws Exception {
      return next.visitStats(ch, this);
   }

   @Override
   public Object perform(Channel ch) throws Exception {
      MemcachedStatsImpl stats = new MemcachedStatsImpl(cache.getAdvancedCache().getStats(), chain);
      
      StringBuilder sb = new StringBuilder();
      writeStat("pid", 0, sb, ch); // Unsupported
      writeStat("uptime", stats.getTimeSinceStart(), sb, ch);
      writeStat("time", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), sb, ch);
      writeStat("version", cache.getVersion(), sb, ch);
      writeStat("pointer_size", 0, sb, ch); // Unsupported
      writeStat("rusage_user", 0, sb, ch); // Unsupported
      writeStat("rusage_system", 0, sb, ch); // Unsupported
      writeStat("curr_items", stats.getCurrentNumberOfEntries(), sb, ch);
      writeStat("total_items", stats.getTotalNumberOfEntries(), sb, ch);
      writeStat("bytes", 0, sb, ch); // Unsupported
      writeStat("curr_connections", 0, sb, ch); // TODO: Through netty?
      writeStat("total_connections", 0, sb, ch); // TODO: Through netty?
      writeStat("connection_structures", 0, sb, ch); // Unsupported
      writeStat("cmd_get", stats.getRetrievals(), sb, ch);
      writeStat("cmd_set", stats.getStores(), sb, ch);
      writeStat("get_hits", stats.getHits(), sb, ch);
      writeStat("get_misses", stats.getMisses(), sb, ch);
      writeStat("delete_misses", stats.getRemoveMisses(), sb, ch);
      writeStat("delete_hits", stats.getRemoveHits(), sb, ch);
      writeStat("incr_misses", stats.getIncrMisses(), sb, ch);
      writeStat("incr_hits", stats.getIncrHits(), sb, ch);
      writeStat("decr_misses", stats.getDecrMisses(), sb, ch);
      writeStat("decr_hits", stats.getDecrHits(), sb, ch);
      writeStat("cas_misses", stats.getCasMisses(), sb, ch);
      writeStat("cas_hits", stats.getCasHits(), sb, ch);
      writeStat("cas_badval", stats.getCasBadval(), sb, ch);
      writeStat("auth_cmds", 0, sb, ch);  // Unsupported
      writeStat("auth_errors", 0, sb, ch); // Unsupported
      //TODO: Evictions are measure by evict calls, but not by nodes are that 
      //      are expired after the entry's lifespan has expired.
      writeStat("evictions", stats.getEvictions(), sb, ch);
      writeStat("bytes_read", 0, sb, ch); // TODO: Through netty?
      writeStat("bytes_written", 0, sb, ch); // TODO: Through netty?
      writeStat("limit_maxbytes", 0, sb, ch); // Unsupported
      writeStat("threads", 0, sb, ch); // TODO: Through netty?
      writeStat("conn_yields", 0, sb, ch); // Unsupported
      
      ch.write(wrappedBuffer(wrappedBuffer(END.toString().getBytes()), wrappedBuffer(CRLF)));
      
      return null;
   }

   private void writeStat(String stat, Object value, StringBuilder sb, Channel ch) {
      sb.append(STAT).append(' ').append(stat).append(' ').append(value);
      ch.write(wrappedBuffer(wrappedBuffer(sb.toString().getBytes()), wrappedBuffer(CRLF)));
      sb.setLength(0);
   }

   public static Command newStatsCommand(Cache cache, CommandType type, InterceptorChain chain) {
      return new StatsCommand(cache, type, chain);
   }

}
