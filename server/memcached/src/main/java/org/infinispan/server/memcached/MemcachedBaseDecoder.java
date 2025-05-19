package org.infinispan.server.memcached;

import static org.infinispan.server.memcached.MemcachedStats.CAS_BADVAL;
import static org.infinispan.server.memcached.MemcachedStats.CAS_HITS;
import static org.infinispan.server.memcached.MemcachedStats.CAS_MISSES;
import static org.infinispan.server.memcached.MemcachedStats.DECR_HITS;
import static org.infinispan.server.memcached.MemcachedStats.DECR_MISSES;
import static org.infinispan.server.memcached.MemcachedStats.INCR_HITS;
import static org.infinispan.server.memcached.MemcachedStats.INCR_MISSES;
import static org.infinispan.server.memcached.binary.BinaryConstants.MAX_EXPIRATION;

import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

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
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.memcached.logging.Header;
import org.infinispan.server.memcached.logging.MemcachedAccessLogging;
import org.infinispan.stats.Stats;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @since 15.0
 **/
public abstract class MemcachedBaseDecoder extends ByteToMessageDecoder {
   protected static final Subject ANONYMOUS = new Subject();
   protected static final Log log = LogFactory.getLog(MemcachedBaseDecoder.class, Log.class);
   protected final MemcachedServer server;
   protected final MemcachedStats statistics;
   protected final boolean statsEnabled;
   protected final boolean accessLogging;
   protected Temporal requestStart;
   protected ChannelHandlerContext ctx;
   protected final TimeService timeService;
   protected final VersionGenerator versionGenerator;
   protected final AdvancedCache<byte[], byte[]> cache;
   protected final Subject subject;
   protected final String principalName;
   protected final ByRef<MemcachedResponse> current = ByRef.create(null);
   protected final int maxContentLength;
   private BiConsumer<ChannelHandlerContext, MemcachedResponse> errorHandler;
   // And this is the ByteBuf pos before decode is performed
   protected int posBefore;

   protected MemcachedBaseDecoder(MemcachedServer server, Subject subject, AdvancedCache<byte[], byte[]> cache) {
      this.server = server;
      this.subject = subject;
      this.principalName = Security.getSubjectUserPrincipalName(subject);
      this.cache = cache.withSubject(subject);
      ComponentRegistry registry = ComponentRegistry.of(cache);
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
      this.maxContentLength = server.getConfiguration().maxContentLengthBytes();
   }

   protected int bytesAvailable(ByteBuf buf, int requestBytes) {
      if (maxContentLength > 0) {
         return Math.max(maxContentLength - requestBytes - buf.readerIndex() + posBefore, 0);
      }
      return Integer.MAX_VALUE;
   }

   public void registerExceptionHandler(BiConsumer<ChannelHandlerContext, MemcachedResponse> handler) {
      this.errorHandler = handler;
   }

   protected final void exceptionCaught(Header header, Throwable t) {
      if (errorHandler != null) {
         errorHandler.accept(ctx, failedResponse(header, t));
      }
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      this.ctx = ctx;
      super.handlerAdded(ctx);
   }

   public void resumeRead() {
      // Also double check auto read in case if enabling auto read caused more bytes to be read which in turn disabled
      // auto read again
      if (internalBuffer().isReadable() && ctx.channel().config().isAutoRead()) {
         // Schedule the read for later to prevent possible StackOverflow
         ctx.channel().eventLoop().submit(() -> {
            try {
               // We HAVE to use our ctx otherwise a read may be in the wrong spot of the pipeline
               channelRead(ctx, Unpooled.EMPTY_BUFFER);
               channelReadComplete(ctx);
            } catch (Throwable t) {
               ctx.fireExceptionCaught(t);
            }
         });
      }
   }

   protected abstract MemcachedResponse failedResponse(Header header, Throwable t);

   protected abstract MemcachedResponse send(Header header, CompletionStage<?> response);

   protected abstract MemcachedResponse send(Header header, CompletionStage<?> response, GenericFutureListener<? extends Future<? super Void>> listener);

   protected Map<byte[], byte[]> statsMap() {
      Stats stats = cache.getAdvancedCache().getStats();
      Map<byte[], byte[]> map = new LinkedHashMap<>(35);
      map.put(MemcachedStats.MemcachedStatsKeys.PID, ParseUtil.writeAsciiLong(ProcessHandle.current().pid()));
      map.put(MemcachedStats.MemcachedStatsKeys.UPTIME, ParseUtil.writeAsciiLong(stats.getTimeSinceStart()));
      map.put(MemcachedStats.MemcachedStatsKeys.TIME, ParseUtil.writeAsciiLong(TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime())));
      map.put(MemcachedStats.MemcachedStatsKeys.VERSION, cache.getVersion().getBytes(StandardCharsets.US_ASCII));
      map.put(MemcachedStats.MemcachedStatsKeys.POINTER_SIZE, ParseUtil.ZERO); // Unsupported
      map.put(MemcachedStats.MemcachedStatsKeys.RUSAGE_USER, ParseUtil.ZERO); // Unsupported
      map.put(MemcachedStats.MemcachedStatsKeys.RUSAGE_SYSTEM, ParseUtil.ZERO); // Unsupported
      map.put(MemcachedStats.MemcachedStatsKeys.CURR_ITEMS, ParseUtil.writeAsciiLong(stats.getApproximateEntries()));
      map.put(MemcachedStats.MemcachedStatsKeys.TOTAL_ITEMS, ParseUtil.writeAsciiLong(stats.getStores()));
      map.put(MemcachedStats.MemcachedStatsKeys.BYTES, ParseUtil.ZERO); // Unsupported
      map.put(MemcachedStats.MemcachedStatsKeys.CMD_GET, ParseUtil.writeAsciiLong(stats.getRetrievals()));
      map.put(MemcachedStats.MemcachedStatsKeys.CMD_SET, ParseUtil.writeAsciiLong(stats.getStores()));
      map.put(MemcachedStats.MemcachedStatsKeys.GET_HITS, ParseUtil.writeAsciiLong(stats.getHits()));
      map.put(MemcachedStats.MemcachedStatsKeys.GET_MISSES, ParseUtil.writeAsciiLong(stats.getMisses()));
      map.put(MemcachedStats.MemcachedStatsKeys.DELETE_MISSES, ParseUtil.writeAsciiLong(stats.getRemoveMisses()));
      map.put(MemcachedStats.MemcachedStatsKeys.DELETE_HITS, ParseUtil.writeAsciiLong(stats.getRemoveHits()));

      if (statsEnabled) {
         map.put(MemcachedStats.MemcachedStatsKeys.INCR_MISSES, ParseUtil.writeAsciiLong(INCR_MISSES.get(statistics)));
         map.put(MemcachedStats.MemcachedStatsKeys.INCR_HITS, ParseUtil.writeAsciiLong(INCR_HITS.get(statistics)));
         map.put(MemcachedStats.MemcachedStatsKeys.DECR_MISSES, ParseUtil.writeAsciiLong(DECR_MISSES.get(statistics)));
         map.put(MemcachedStats.MemcachedStatsKeys.DECR_HITS, ParseUtil.writeAsciiLong(DECR_HITS.get(statistics)));
         map.put(MemcachedStats.MemcachedStatsKeys.CAS_MISSES, ParseUtil.writeAsciiLong(CAS_MISSES.get(statistics)));
         map.put(MemcachedStats.MemcachedStatsKeys.CAS_HITS, ParseUtil.writeAsciiLong(CAS_HITS.get(statistics)));
         map.put(MemcachedStats.MemcachedStatsKeys.CAS_BADVAL, ParseUtil.writeAsciiLong(CAS_BADVAL.get(statistics)));
      }

      map.put(MemcachedStats.MemcachedStatsKeys.AUTH_CMDS, ParseUtil.ZERO); // Unsupported
      map.put(MemcachedStats.MemcachedStatsKeys.AUTH_ERRORS, ParseUtil.ZERO); // Unsupported
      //TODO: Evictions are measured by evict calls, but not by nodes are that are expired after the entry's lifespan has expired.
      map.put(MemcachedStats.MemcachedStatsKeys.EVICTIONS, ParseUtil.writeAsciiLong(stats.getEvictions()));

      NettyTransport transport = server.getTransport();
      if (transport == null) {
         transport = (NettyTransport) server.getEnclosingProtocolServer().getTransport();
      }

      map.put(MemcachedStats.MemcachedStatsKeys.BYTES_READ, ParseUtil.writeAsciiLong(transport.getTotalBytesRead()));
      map.put(MemcachedStats.MemcachedStatsKeys.BYTES_WRITTEN, ParseUtil.writeAsciiLong(transport.getTotalBytesWritten()));
      map.put(MemcachedStats.MemcachedStatsKeys.CURR_CONNECTIONS, ParseUtil.writeAsciiLong(transport.getNumberOfLocalConnections()));
      map.put(MemcachedStats.MemcachedStatsKeys.TOTAL_CONNECTIONS, ParseUtil.writeAsciiLong(transport.getNumberOfGlobalConnections()));
      map.put(MemcachedStats.MemcachedStatsKeys.THREADS, ParseUtil.ZERO); // TODO: Through netty?
      map.put(MemcachedStats.MemcachedStatsKeys.CONNECTION_STRUCTURES, ParseUtil.ZERO); // Unsupported
      map.put(MemcachedStats.MemcachedStatsKeys.LIMIT_MAXBYTES, ParseUtil.ZERO); // Unsupported
      map.put(MemcachedStats.MemcachedStatsKeys.CONN_YIELDS, ParseUtil.ZERO); // Unsupported
      map.put(MemcachedStats.MemcachedStatsKeys.RECLAIMED, ParseUtil.ZERO); // Unsupported
      return map;
   }

   protected Metadata touchMetadata(CacheEntry<?, ?> entry, int expiration) {
      return new MemcachedMetadata.Builder()
            .merge(entry.getMetadata())
            .lifespan(expiration > 0 ? toMillis(expiration) : -1)
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
