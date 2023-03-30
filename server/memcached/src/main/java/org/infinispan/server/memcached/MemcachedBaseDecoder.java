package org.infinispan.server.memcached;

import static org.infinispan.server.memcached.MemcachedStats.CAS_BADVAL;
import static org.infinispan.server.memcached.MemcachedStats.CAS_HITS;
import static org.infinispan.server.memcached.MemcachedStats.CAS_MISSES;
import static org.infinispan.server.memcached.MemcachedStats.DECR_HITS;
import static org.infinispan.server.memcached.MemcachedStats.DECR_MISSES;
import static org.infinispan.server.memcached.MemcachedStats.INCR_HITS;
import static org.infinispan.server.memcached.MemcachedStats.INCR_MISSES;
import static org.infinispan.server.memcached.binary.BinaryConstants.MAX_EXPIRATION;

import java.time.temporal.Temporal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.security.Security;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.memcached.logging.Header;
import org.infinispan.server.memcached.logging.Log;
import org.infinispan.server.memcached.logging.MemcachedAccessLogging;
import org.infinispan.stats.Stats;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @since 15.0
 **/
public abstract class MemcachedBaseDecoder extends ByteToMessageDecoder {
   protected final static Subject ANONYMOUS = new Subject();
   protected final static Log log = LogFactory.getLog(MemcachedBaseDecoder.class, Log.class);
   protected final MemcachedServer server;
   protected final MemcachedStats statistics;
   protected final boolean statsEnabled;
   protected final boolean accessLogging;
   protected Temporal requestStart;
   protected Channel channel;
   protected final TimeService timeService;
   protected final VersionGenerator versionGenerator;
   protected final AdvancedCache<byte[], byte[]> cache;
   protected final Subject subject;
   protected final String principalName;
   protected final ByRef<MemcachedResponse> current = ByRef.create(null);

   protected MemcachedBaseDecoder(MemcachedServer server, Subject subject, AdvancedCache<byte[], byte[]> cache) {
      this.server = server;
      this.subject = subject;
      this.principalName = Security.getSubjectUserPrincipalName(subject);
      this.cache = cache.withSubject(subject);
      ComponentRegistry registry = cache.getComponentRegistry();
      VersionGenerator versionGenerator = registry.getComponent(VersionGenerator.class);
      if (versionGenerator == null) {
         versionGenerator = new NumericVersionGenerator();
         registry.registerComponent(versionGenerator, VersionGenerator.class);
      }
      this.versionGenerator = versionGenerator;
      this.timeService = registry.getTimeService();
      this.statistics = server.getStatistics();
      this.statsEnabled = statistics != null;
      this.accessLogging = MemcachedAccessLogging.isEnabled();
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) {
      channel = ctx.channel();
   }

   protected abstract void send(Header header, CompletionStage<?> response);

   protected abstract void send(Header header, CompletionStage<?> response, GenericFutureListener<? extends Future<? super Void>> listener);

   protected Map<String, String> statsMap() {
      Stats stats = cache.getAdvancedCache().getStats();
      Map<String, String> map = new LinkedHashMap<>(35);
      map.put("pid", Long.toString(ProcessHandle.current().pid()));
      map.put("uptime", Long.toString(stats.getTimeSinceStart()));
      map.put("time", Long.toString(TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime())));
      map.put("version", cache.getVersion());
      map.put("pointer_size", "0"); // Unsupported
      map.put("rusage_user", "0"); // Unsupported
      map.put("rusage_system", "0"); // Unsupported
      map.put("curr_items", Long.toString(stats.getApproximateEntries()));
      map.put("total_items", Long.toString(stats.getStores()));
      map.put("bytes", "0"); // Unsupported
      map.put("cmd_get", Long.toString(stats.getRetrievals()));
      map.put("cmd_set", Long.toString(stats.getStores()));
      map.put("get_hits", Long.toString(stats.getHits()));
      map.put("get_misses", Long.toString(stats.getMisses()));
      map.put("delete_misses", Long.toString(stats.getRemoveMisses()));
      map.put("delete_hits", Long.toString(stats.getRemoveHits()));
      map.put("incr_misses", Long.toString(INCR_MISSES.get(statistics)));
      map.put("incr_hits", Long.toString(INCR_HITS.get(statistics)));
      map.put("decr_misses", Long.toString(DECR_MISSES.get(statistics)));
      map.put("decr_hits", Long.toString(DECR_HITS.get(statistics)));
      map.put("cas_misses", Long.toString(CAS_MISSES.get(statistics)));
      map.put("cas_hits", Long.toString(CAS_HITS.get(statistics)));
      map.put("cas_badval", Long.toString(CAS_BADVAL.get(statistics)));
      map.put("auth_cmds", "0"); // Unsupported
      map.put("auth_errors", "0"); // Unsupported
      //TODO: Evictions are measured by evict calls, but not by nodes are that are expired after the entry's lifespan has expired.
      map.put("evictions", Long.toString(stats.getEvictions()));
      NettyTransport transport = server.getTransport();
      map.put("bytes_read", Long.toString(transport.getTotalBytesRead()));
      map.put("bytes_written", Long.toString(transport.getTotalBytesWritten()));
      map.put("curr_connections", Long.toString(transport.getNumberOfLocalConnections()));
      map.put("total_connections", Long.toString(transport.getNumberOfGlobalConnections()));
      map.put("threads", "0"); // TODO: Through netty?
      map.put("connection_structures", "0"); // Unsupported
      map.put("limit_maxbytes", "0"); // Unsupported
      map.put("conn_yields", "0"); // Unsupported
      map.put("reclaimed", "0"); // Unsupported
      return map;
   }

   protected Metadata touchMetadata(CacheEntry<?, ?> entry, int expiration) {
      return new MemcachedMetadata.Builder()
            .merge(entry.getMetadata())
            .lifespan(toMillis(expiration))
            .build();
   }

   protected long toMillis(int lifespan) {
      if (lifespan > MAX_EXPIRATION) {
         long unixTimeExpiry = TimeUnit.SECONDS.toMillis(lifespan) - timeService.wallClockTime();
         return unixTimeExpiry < 0 ? 0 : unixTimeExpiry;
      } else {
         return TimeUnit.SECONDS.toMillis(lifespan);
      }
   }

   protected Metadata metadata(int flags, int expiration) {
      return new MemcachedMetadata.Builder()
            .flags(flags)
            .version(versionGenerator.generateNew())
            .lifespan(expiration > 0 ? toMillis(expiration) : -1)
            .build();
   }
}
