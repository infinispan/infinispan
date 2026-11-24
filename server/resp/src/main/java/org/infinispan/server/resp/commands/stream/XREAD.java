package org.infinispan.server.resp.commands.stream;

import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.isToken;
import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.matches;
import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.writeEntries;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.encoding.DataConversion;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.RespVersion;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.filter.EventListenerConverter;
import org.infinispan.server.resp.filter.EventListenerKeysFilter;
import org.infinispan.server.resp.meta.ClientMetadata;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.stream.StreamBucket;
import org.infinispan.server.resp.stream.StreamEntry;
import org.infinispan.server.resp.stream.StreamID;
import org.infinispan.server.resp.tx.TransactionContext;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * XREAD
 *
 * <p>
 * Read data from one or more streams, returning only entries with an ID greater than the last received ID reported by
 * the caller. The special {@code $} ID reads only entries added after the call, and the special {@code +} ID reads
 * the last entry in the stream. With the {@code BLOCK} option, the command blocks until new data is available or the
 * timeout elapses, returning a nil reply on timeout. Unlike the blocking list operations, the entries are not removed
 * from the stream: every client blocked on the stream is served with the new data.
 * </p>
 *
 * @see <a href="https://redis.io/commands/xread/">XREAD</a>
 * @since 16.4
 */
public class XREAD extends RespCommand implements Resp3Command {

   private static final byte[] COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MAXCOUNT = "MAXCOUNT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MAXSIZE = "MAXSIZE".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] BLOCK = "BLOCK".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] STREAMS = "STREAMS".getBytes(StandardCharsets.US_ASCII);

   public XREAD() {
      super(-4, 0, 0, 0,
            AclCategory.READ.mask() | AclCategory.STREAM.mask() | AclCategory.SLOW.mask() | AclCategory.BLOCKING.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      // XREAD [COUNT count] [MAXCOUNT maxcount] [MAXSIZE maxsize] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
      long count = -1;
      long maxcount = -1;
      long maxsize = -1;
      long block = -1;
      int streamsPos = -1;

      int pos = 0;
      while (pos < arguments.size()) {
         byte[] argument = arguments.get(pos++);
         if (matches(argument, COUNT)) {
            count = parseCount(arguments, pos);
            pos++;
         } else if (matches(argument, MAXCOUNT)) {
            maxcount = parsePositiveLong(arguments, pos, "MAXCOUNT");
            pos++;
         } else if (matches(argument, MAXSIZE)) {
            maxsize = parsePositiveLong(arguments, pos, "MAXSIZE");
            pos++;
         } else if (matches(argument, BLOCK)) {
            block = parseNonNegativeLong(arguments, pos);
            pos++;
         } else if (matches(argument, STREAMS)) {
            streamsPos = pos;
            break;
         } else {
            throw new RespCommandException("ERR syntax error");
         }
      }

      if (streamsPos < 0) {
         throw new RespCommandException("ERR syntax error, the STREAMS option is mandatory");
      }

      if (maxcount > 0 && count > 0 && maxcount < count) {
         throw new RespCommandException("ERR MAXCOUNT must be greater than or equal to COUNT");
      }

      int rest = arguments.size() - streamsPos;
      if (rest < 2 || rest % 2 != 0) {
         throw new RespCommandException(
               "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.");
      }

      int numKeys = rest / 2;
      List<ReadPair> pairs = new ArrayList<>(numKeys);
      for (int i = 0; i < numKeys; i++) {
         byte[] key = arguments.get(streamsPos + i);
         byte[] idArgument = arguments.get(streamsPos + numKeys + i);
         if (isToken(idArgument, '$')) {
            pairs.add(new ReadPair(key, false, null));
         } else if (isToken(idArgument, '+')) {
            pairs.add(new ReadPair(key, true, null));
         } else {
            try {
               pairs.add(new ReadPair(key, false, StreamID.parse(idArgument)));
            } catch (NumberFormatException e) {
               throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
            }
         }
      }

      // Resolve the special '$' ID to the current last ID of each stream, before any read. The '$' ID means: only
      // the entries added after this command.
      final long countArg = count;
      final long maxcountArg = maxcount;
      final long maxsizeArg = maxsize;
      final long blockArg = block;
      CompletionStage<Void> resolution = resolveDollarIds(handler, pairs);
      CompletionStage<List<ReadResult>> read = resolution
            .thenCompose(ignore -> readAll(handler, pairs, countArg, maxcountArg, maxsizeArg));

      if (blockArg < 0 || TransactionContext.isInTransactionContext(ctx)) {
         // Non-blocking utilization, or invoked from MULTI/EXEC, which must not block.
         return handler.stageToReturn(read, ctx, XREAD_WRITER);
      }

      return handler.stageToReturn(read.thenCompose(results -> {
         // When there is data to return, the command is executed synchronously, exactly like the BLOCK option was
         // missing. Otherwise, install a listener to wait for new data.
         return results.isEmpty()
               ? blockForData(handler, pairs, countArg, maxcountArg, maxsizeArg, blockArg, ctx)
               : CompletableFuture.completedFuture(results);
      }), ctx, XREAD_WRITER);
   }

   private static long parsePositiveLong(List<byte[]> arguments, int pos, String option) {
      if (pos >= arguments.size()) {
         throw new RespCommandException("ERR syntax error");
      }
      long value = ArgumentUtils.toLong(arguments.get(pos));
      if (value <= 0) {
         throw new RespCommandException("ERR " + option + " must be a positive integer");
      }
      return value;
   }

   /**
    * Parses the {@code COUNT} option. Unlike {@code MAXCOUNT} and {@code MAXSIZE}, Redis accepts non-positive values,
    * normalizing them to an unlimited read.
    */
   private static long parseCount(List<byte[]> arguments, int pos) {
      if (pos >= arguments.size()) {
         throw new RespCommandException("ERR syntax error");
      }
      long value = ArgumentUtils.toLong(arguments.get(pos));
      return value > 0 ? value : -1;
   }

   private static long parseNonNegativeLong(List<byte[]> arguments, int pos) {
      if (pos >= arguments.size()) {
         throw new RespCommandException("ERR syntax error");
      }
      long value = ArgumentUtils.toLong(arguments.get(pos));
      if (value < 0) {
         throw new RespCommandException("ERR timeout is negative");
      }
      return value;
   }

   private static CompletionStage<Void> resolveDollarIds(Resp3Handler handler, List<ReadPair> pairs) {
      List<CompletableFuture<?>> resolutions = new ArrayList<>(pairs.size());
      for (ReadPair pair : pairs) {
         if (pair.lastId == null && !pair.last) {
            resolutions.add(handler.getStreamCache().bucket(pair.key).toCompletableFuture().thenAccept(bucket -> {
               // A missing stream means the '$' ID resolves to the minimum, so any future entry is returned.
               pair.lastId = bucket == null ? StreamID.ZERO : bucket.lastId();
            }));
         }
      }
      return CompletableFuture.allOf(resolutions.toArray(new CompletableFuture[0]));
   }

   /**
    * Reads all the pairs in order, applying the per-stream {@code COUNT}, and the cumulative {@code MAXCOUNT} and
    * {@code MAXSIZE} budgets. Only the pairs with at least one entry returned are included in the result.
    */
   private static CompletionStage<List<ReadResult>> readAll(Resp3Handler handler, List<ReadPair> pairs, long count,
         long maxcount, long maxsize) {
      return readPairs(handler, pairs.iterator(), new ArrayList<>(pairs.size()), 0, 0, count, maxcount, maxsize);
   }

   private static CompletionStage<List<ReadResult>> readPairs(Resp3Handler handler, Iterator<ReadPair> iterator,
         List<ReadResult> results, long totalEntries, long totalSize, long count, long maxcount, long maxsize) {
      if (!iterator.hasNext()) {
         return CompletableFuture.completedFuture(results);
      }

      ReadPair pair = iterator.next();
      CompletionStage<List<StreamEntry>> stage = pair.last
            ? handler.getStreamCache().readLast(pair.key)
            : handler.getStreamCache().read(pair.key, pair.lastId, count);
      return stage.thenCompose(entries -> {
         List<StreamEntry> limited = limitEntries(entries, totalEntries, totalSize, maxcount, maxsize);
         if (!limited.isEmpty()) {
            results.add(new ReadResult(pair.key, limited));
         }
         return readPairs(handler, iterator, results, totalEntries + limited.size(),
               totalSize + replySize(limited), count, maxcount, maxsize);
      });
   }

   /**
    * Applies the cumulative {@code MAXCOUNT} and {@code MAXSIZE} budgets to the entries of a single stream. At least
    * one entry is always returned across the whole command, even when it exceeds the {@code MAXSIZE} budget.
    */
   private static List<StreamEntry> limitEntries(List<StreamEntry> entries, long totalEntries, long totalSize,
         long maxcount, long maxsize) {
      if (entries.isEmpty()) {
         return entries;
      }

      List<StreamEntry> limited = entries;
      if (maxcount > 0) {
         long remaining = maxcount - totalEntries;
         if (remaining <= 0) {
            return List.of();
         }
         if (limited.size() > remaining) {
            limited = limited.subList(0, (int) remaining);
         }
      }

      if (maxsize > 0) {
         List<StreamEntry> fitting = new ArrayList<>(limited.size());
         long size = totalSize;
         for (StreamEntry entry : limited) {
            long entrySize = entry.replySize();
            // The budget is never enforced for the first entry of the whole reply.
            if (!fitting.isEmpty() || totalEntries > 0) {
               if (size + entrySize > maxsize) {
                  break;
               }
            }
            fitting.add(entry);
            size += entrySize;
         }
         limited = fitting;
      }
      return limited;
   }

   private static long replySize(List<StreamEntry> entries) {
      long size = 0;
      for (StreamEntry entry : entries) {
         size += entry.replySize();
      }
      return size;
   }

   /**
    * Installs a listener to wait for new data on any of the requested streams, following the same approach as the
    * blocking list commands. A timer handles the timeout, completing the command with a nil reply. The blocked read
    * is also cancelled when the client disconnects, releasing the listener and the blocked-client metrics even
    * without a timeout, as with {@code BLOCK 0}.
    */
   private CompletionStage<List<ReadResult>> blockForData(Resp3Handler handler, List<ReadPair> pairs, long count,
         long maxcount, long maxsize, long block, ChannelHandlerContext ctx) {
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);
      DataConversion vc = cache.getValueDataConversion();
      XReadListener listener = new XReadListener(handler, cache, pairs, count, maxcount, maxsize);
      EventListenerKeysFilter filter = new EventListenerKeysFilter(pairs.stream().map(p -> p.key));
      long deadline = block > 0
            ? handler.respServer().getTimeService().expectedEndTime(block, TimeUnit.MILLISECONDS)
            : 0;

      // Cancel the blocked read when the channel closes: without new data, an error or a timeout, the command future
      // is never completed, and the listener would retain the handler and cache references indefinitely. The close
      // future listener runs immediately when the channel is already closed, covering a disconnect racing with the
      // command itself. The listener is removed when the command completes, so repeated blocked reads on a
      // long-lived connection do not accumulate on the close future until the channel closes.
      ChannelFuture closeFuture = ctx.channel().closeFuture();
      GenericFutureListener<Future<? super Void>> cancelOnClose = ignored -> listener.getFuture().cancel(false);
      closeFuture.addListener(cancelOnClose);
      listener.getFuture().whenComplete((ignore, t) -> closeFuture.removeListener(cancelOnClose));

      cache.addListenerAsync(listener, filter, new EventListenerConverter<Object, Object, Object>(vc))
            .whenComplete((ignore, t) -> {
               // If the listener fails to install, fail the command.
               if (t != null) {
                  listener.getFuture().completeExceptionally(t);
                  return;
               }
               // The client may have disconnected while the listener was being installed, cancelling the command: the
               // removal triggered by the cancellation ran before the registration completed, so remove it here.
               if (listener.getFuture().isDone()) {
                  cache.removeListenerAsync(listener);
                  return;
               }
               // The listener can lose events during its installation, so we need to poll again. Start a timer with
               // the remaining time, accounting for the time spent installing the listener.
               if (block > 0) {
                  long remaining = handler.respServer().getTimeService().remainingTime(deadline, TimeUnit.MILLISECONDS);
                  if (remaining <= 0) {
                     cache.removeListenerAsync(listener);
                     listener.getFuture().complete(null);
                     return;
                  }
                  listener.startTimer(remaining);
               }
               listener.synchronizer.onListenerAdded();
            });

      ClientMetadata metadata = handler.respServer().metadataRepository().client();
      metadata.incrementBlockedClients();
      metadata.recordBlockedKeys(pairs.size());
      listener.getFuture().whenComplete((ignore, t) -> {
         metadata.decrementBlockedClients();
         metadata.recordBlockedKeys(-pairs.size());
      });
      return listener.getFuture();
   }

   /**
    * A pair of stream key and the last ID received by the caller. The {@code lastId} is {@code null} until the
    * special {@code $} ID is resolved, and the {@code last} flag marks the special {@code +} ID.
    */
   private static final class ReadPair {
      final byte[] key;
      final boolean last;
      StreamID lastId;

      ReadPair(byte[] key, boolean last, StreamID lastId) {
         this.key = key;
         this.last = last;
         this.lastId = lastId;
      }
   }

   record ReadResult(byte[] key, List<StreamEntry> entries) {
   }

   /**
    * Writes the XREAD reply: a map of key to entries in RESP3, or an array of key and entries pairs in RESP2. A nil
    * reply is written when there is no stream that can be served.
    */
   private static final JavaObjectSerializer<List<ReadResult>> XREAD_WRITER = new JavaObjectSerializer<>() {
      @Override
      public void accept(List<ReadResult> results, ResponseWriter writer) {
         if (results == null || results.isEmpty()) {
            writer.nulls();
            return;
         }

         if (writer.version() == RespVersion.RESP3) {
            writer.writeNumericPrefix(RespConstants.MAP, results.size());
            for (ReadResult result : results) {
               writer.string(result.key());
               writeEntries(result.entries(), writer);
            }
         } else {
            writer.arrayStart(results.size());
            for (ReadResult result : results) {
               writer.arrayStart(2);
               writer.string(result.key());
               writeEntries(result.entries(), writer);
               writer.arrayEnd();
            }
            writer.arrayEnd();
         }
      }
   };

   @Listener(clustered = true)
   public static final class XReadListener {
      private final AdvancedCache<byte[], Object> cache;
      private final Resp3Handler handler;
      private volatile ScheduledFuture<?> scheduledTimer;
      final XReadSynchronizer synchronizer;

      XReadListener(Resp3Handler handler, AdvancedCache<byte[], Object> cache, List<ReadPair> pairs, long count,
            long maxcount, long maxsize) {
         this.handler = handler;
         this.cache = cache;
         this.synchronizer = new XReadSynchronizer(handler, pairs, count, maxcount, maxsize);

         synchronizer.resultFuture.whenComplete((ignore, t) -> {
            deleteTimer();
            cache.removeListenerAsync(this);
         });
      }

      CompletableFuture<List<ReadResult>> getFuture() {
         return synchronizer.resultFuture;
      }

      void startTimer(long timeout) {
         deleteTimer();
         scheduledTimer = handler.getScheduler().schedule(() -> {
            cache.removeListenerAsync(this);
            synchronizer.resultFuture.complete(null);
         }, timeout, TimeUnit.MILLISECONDS);
      }

      private void deleteTimer() {
         if (scheduledTimer != null) {
            scheduledTimer.cancel(true);
         }
         scheduledTimer = null;
      }

      @CacheEntryCreated
      @CacheEntryModified
      public void onEvent(CacheEntryEvent<Object, Object> entryEvent) {
         try {
            if (entryEvent.getValue() instanceof StreamBucket) {
               synchronizer.onEvent();
            }
         } catch (Exception ex) {
            synchronizer.resultFuture.completeExceptionally(ex);
         }
      }
   }

   /**
    * Synchronizes the access to the result future: it is completed only when a read returns at least one entry, or
    * when the timeout elapses. Empty reads keep the command blocked, as the blocked client must not reply with an
    * empty array.
    */
   static final class XReadSynchronizer {
      final CompletableFuture<List<ReadResult>> resultFuture = new CompletableFuture<>();
      private final Resp3Handler handler;
      private final List<ReadPair> pairs;
      private final long count;
      private final long maxcount;
      private final long maxsize;

      XReadSynchronizer(Resp3Handler handler, List<ReadPair> pairs, long count, long maxcount, long maxsize) {
         this.handler = handler;
         this.pairs = pairs;
         this.count = count;
         this.maxcount = maxcount;
         this.maxsize = maxsize;
      }

      void onListenerAdded() {
         reRead();
      }

      void onEvent() {
         reRead();
      }

      private void reRead() {
         if (resultFuture.isDone()) {
            return;
         }
         readAll(handler, pairs, count, maxcount, maxsize).whenComplete((results, t) -> {
            if (t != null) {
               // Between installing the listener and reading, the key type may have changed. In this case, keep the
               // command blocked until the timeout, the same behavior as the blocking list commands.
               if (!RespUtil.isWrongTypeError(t)) {
                  resultFuture.completeExceptionally(t);
               }
               return;
            }
            if (results != null && !results.isEmpty()) {
               resultFuture.complete(results);
            }
            // An empty read keeps the command blocked, waiting for the next event or the timeout.
         });
      }
   }
}
