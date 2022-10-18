package org.infinispan.server.memcached;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.util.concurrent.CompletableFutures.asCompletionException;
import static org.infinispan.server.core.transport.ExtendedByteBuf.buffer;
import static org.infinispan.server.core.transport.ExtendedByteBuf.wrappedBuffer;
import static org.infinispan.server.memcached.TextProtocolUtil.CLIENT_ERROR_BAD_FORMAT;
import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;
import static org.infinispan.server.memcached.TextProtocolUtil.CRLFBytes;
import static org.infinispan.server.memcached.TextProtocolUtil.DELETED;
import static org.infinispan.server.memcached.TextProtocolUtil.END;
import static org.infinispan.server.memcached.TextProtocolUtil.END_SIZE;
import static org.infinispan.server.memcached.TextProtocolUtil.ERROR;
import static org.infinispan.server.memcached.TextProtocolUtil.EXISTS;
import static org.infinispan.server.memcached.TextProtocolUtil.MAX_UNSIGNED_LONG;
import static org.infinispan.server.memcached.TextProtocolUtil.MIN_UNSIGNED;
import static org.infinispan.server.memcached.TextProtocolUtil.NOT_FOUND;
import static org.infinispan.server.memcached.TextProtocolUtil.NOT_STORED;
import static org.infinispan.server.memcached.TextProtocolUtil.OK;
import static org.infinispan.server.memcached.TextProtocolUtil.SERVER_ERROR;
import static org.infinispan.server.memcached.TextProtocolUtil.SP;
import static org.infinispan.server.memcached.TextProtocolUtil.STORED;
import static org.infinispan.server.memcached.TextProtocolUtil.TOUCHED;
import static org.infinispan.server.memcached.TextProtocolUtil.VALUE;
import static org.infinispan.server.memcached.TextProtocolUtil.VALUE_SIZE;
import static org.infinispan.server.memcached.TextProtocolUtil.ZERO;
import static org.infinispan.server.memcached.TextProtocolUtil.concat;
import static org.infinispan.server.memcached.TextProtocolUtil.extractString;
import static org.infinispan.server.memcached.TextProtocolUtil.readDiscardedLine;
import static org.infinispan.server.memcached.TextProtocolUtil.readElement;
import static org.infinispan.server.memcached.TextProtocolUtil.readSplitLine;
import static org.infinispan.server.memcached.TextProtocolUtil.skipLine;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.memcached.logging.Log;
import org.infinispan.stats.Stats;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.CharsetUtil;

/**
 * A Memcached protocol specific decoder
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class MemcachedDecoder extends ReplayingDecoder<MemcachedDecoderState> {

   public MemcachedDecoder(AdvancedCache<byte[], byte[]> memcachedCache, ScheduledExecutorService scheduler,
                           NettyTransport transport, Predicate<? super String> ignoreCache,
                           MediaType valuePayload) {

      super(MemcachedDecoderState.DECODE_HEADER);
      this.cache = memcachedCache.withMediaType(TEXT_PLAIN, valuePayload);
      this.scheduler = scheduler;
      this.transport = transport;
      this.ignoreCache = ignoreCache;
      isStatsEnabled = cache.getCacheConfiguration().statistics().enabled();
      this.timeService = memcachedCache.getComponentRegistry().getTimeService();
   }

   private final AdvancedCache<byte[], byte[]> cache;
   private final ScheduledExecutorService scheduler;
   protected final NettyTransport transport;
   protected final Predicate<? super String> ignoreCache;
   private final TimeService timeService;

   private final static Log log = LogFactory.getLog(MemcachedDecoder.class, Log.class);

   public static final int SecondsInAMonth = 60 * 60 * 24 * 30;

   long defaultLifespanTime;
   long defaultMaxIdleTime;

   protected byte[] key;
   protected byte[] rawValue;
   protected Configuration cacheConfiguration;

   protected MemcachedParameters params;
   private final boolean isStatsEnabled;
   private final AtomicLong incrMisses = new AtomicLong();
   private final AtomicLong incrHits = new AtomicLong();
   private final AtomicLong decrMisses = new AtomicLong();
   private final AtomicLong decrHits = new AtomicLong();
   private final AtomicLong replaceIfUnmodifiedMisses = new AtomicLong();
   private final AtomicLong replaceIfUnmodifiedHits = new AtomicLong();
   private final AtomicLong replaceIfUnmodifiedBadval = new AtomicLong();
   private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
   protected RequestHeader header;
   ResponseEntry lastResponse;

   private static MemcachedException wrapBadFormat(Throwable throwable) {
      return new MemcachedException(CLIENT_ERROR_BAD_FORMAT + throwable.getMessage(), throwable);
   }

   private static MemcachedException wrapServerError(Throwable throwable) {
      return new MemcachedException(SERVER_ERROR + throwable.getMessage(), throwable);
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      try {
         if (log.isTraceEnabled()) // To aid debugging
            log.tracef("Decode using instance @%x", System.identityHashCode(this));
         MemcachedDecoderState state = state();
         switch (state) {
            case DECODE_HEADER:
               decodeHeader(ctx, in, out);
               break;
            case DECODE_KEY:
               decodeKey(ctx, in);
               break;
            case DECODE_PARAMETERS:
               decodeParameters(ctx, in);
               break;
            case DECODE_VALUE:
               decodeValue(ctx, in);
               break;
         }
      } catch (IOException | NumberFormatException e) {
         ctx.pipeline().fireExceptionCaught(wrapBadFormat(e));
         resetParams();
      } catch (Exception e) {
         ctx.pipeline().fireExceptionCaught(wrapServerError(e));
         resetParams();
      }
   }

   void decodeHeader(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out)
         throws CacheUnavailableException, IOException {
      if (log.isTraceEnabled()) {
         log.tracef("Decode header using instance @%x", System.identityHashCode(this));
      }
      header = new RequestHeader();
      boolean endOfOp = readHeader(buffer, header);
      Channel ch = ctx.channel();
      String cacheName = cache.getName();
      if (ignoreCache.test(cacheName)) throw new CacheUnavailableException(cacheName);
      cacheConfiguration = getCacheConfiguration();
      defaultLifespanTime = cacheConfiguration.expiration().lifespan();
      defaultMaxIdleTime = cacheConfiguration.expiration().maxIdle();
      if (endOfOp) {
         if (header.operation == MemcachedOperation.StatsRequest) {
            sendResponseOrdered(ch, CompletableFuture.completedFuture(createStatsResponse()));
         } else {
            customDecodeHeader(ctx, buffer);
         }
      } else {
         checkpoint(MemcachedDecoderState.DECODE_KEY);
      }
   }

   void decodeKey(ChannelHandlerContext ctx, ByteBuf buffer) throws IOException {
      if (log.isTraceEnabled()) {
         log.tracef("Decoding key using instance @%x", System.identityHashCode(this));
      }
      Channel ch = ctx.channel();
      switch (header.operation) {
         case GetRequest:
         case GetWithVersionRequest:
            get(buffer, ch);
            break;
         case PutRequest:
         case TouchRequest:
         case RemoveRequest:
         case PutIfAbsentRequest:
         case ReplaceRequest:
         case ReplaceIfUnmodifiedRequest:
            handleModification(ch, buffer);
            break;
         default:
            customDecodeKey(ctx, buffer);
            break;
      }
   }

   private void decodeParameters(ChannelHandlerContext ctx, ByteBuf buffer) throws IOException {
      if (log.isTraceEnabled()) {
         log.tracef("Decoding parameters using instance @%x", System.identityHashCode(this));
      }
      boolean endOfOp = readParameters(buffer);
      if (!endOfOp && params.valueLength > 0) {
         // Create value holder and checkpoint only if there's more to read
         rawValue = new byte[params.valueLength];
         checkpoint(MemcachedDecoderState.DECODE_VALUE);
      } else if (params.valueLength == 0) {
         rawValue = Util.EMPTY_BYTE_ARRAY;
         decodeValue(ctx, buffer);
      } else {
         decodeValue(ctx, buffer);
      }
   }

   private void decodeValue(ChannelHandlerContext ctx, ByteBuf buffer) {
      if (log.isTraceEnabled()) {
         log.tracef("Decoding value using instance @%x", System.identityHashCode(this));
      }
      Channel ch = ctx.channel();
      switch (header.operation) {
         case PutRequest:
            readValue(buffer);
            put(ch);
            break;
         case TouchRequest:
            touch(ch);
            break;
         case PutIfAbsentRequest:
            readValue(buffer);
            putIfAbsent(ch);
            break;
         case ReplaceRequest:
            readValue(buffer);
            replace(ch);
            break;
         case ReplaceIfUnmodifiedRequest:
            readValue(buffer);
            replaceIfUnmodified(ch);
            break;
         case RemoveRequest:
            remove(ch);
            break;
         default:
            customDecodeValue(ctx, buffer);
            break;
      }
   }

   protected void replace(Channel ch) {
      try {
         OperationContext ctx = createOperationContext();
         // Avoid listener notification for a simple optimization
         // on whether a new version should be calculated or not.
         sendResponseOrdered(ch, cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION)
               .getAsync(ctx.key)
               .thenCompose(prev -> prev == null ?
                     CompletableFutures.completedNull() :
                     cache.replaceAsync(ctx.key, ctx.value, buildMetadata(ctx)))
               .thenApply(prev -> prev == null ? createNotExecutedResponse(ctx) : createSuccessResponse(ctx)));
      } finally {
         resetParams();
      }
   }

   protected void replaceIfUnmodified(Channel ch) {
      try {
         OperationContext ctx = createOperationContext();

         sendResponseOrdered(ch, cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION)
               .getCacheEntryAsync(ctx.key)
               .thenCompose(entry -> {
                  if (entry == null) {
                     return completedFuture(createNotExistResponse(ctx));
                  }
                  NumericVersion streamVersion = new NumericVersion(ctx.parameters.streamVersion);
                  if (!entry.getMetadata().version().equals(streamVersion)) {
                     return completedFuture(createNotExecutedResponse(ctx));
                  }
                  byte[] prev = entry.getValue();
                  return cache.replaceAsync(ctx.key, prev, ctx.value, buildMetadata(ctx))
                        .thenApply(replaced -> replaced ? createSuccessResponse(ctx) : createNotExecutedResponse(ctx));
               }));
      } finally {
         resetParams();
      }
   }

   private void touch(Channel ch) {
      try {
         OperationContext ctx = createOperationContext();
         sendResponseOrdered(ch, cache.getCacheEntryAsync(ctx.key)
               .thenCompose(cacheEntry -> {
                  if (cacheEntry == null) {
                     return completedFuture(createNotExistResponse(ctx));
                  }
                  return cache.replaceAsync(cacheEntry.getKey(), cacheEntry.getValue(), touchMetadata(ctx, cacheEntry))
                        .thenApply(bytes -> createTouchedResponse(ctx));
               }));
      } finally {
         resetParams();
      }
   }

   private void putIfAbsent(Channel ch) {
      try {
         OperationContext ctx = createOperationContext();
         // TODO why not just invoking putIfAbsent?
         sendResponseOrdered(ch, cache.getAsync(ctx.key)
               .thenCompose(prev -> prev == null ?
                     cache.putIfAbsentAsync(ctx.key, ctx.value, buildMetadata(ctx)) :
                     completedFuture(prev))
               .thenApply(prev -> prev == null ? createSuccessResponse(ctx) : createNotExecutedResponse(ctx)));
      } finally {
         resetParams();
      }
   }

   private void put(Channel ch) {
      try {
         OperationContext ctx = createOperationContext();
         // Get an optimised cache in case we can make the operation more efficient
         sendResponseOrdered(ch, cache.withFlags(Flag.IGNORE_RETURN_VALUES)
               .putAsync(ctx.key, ctx.value, buildMetadata(ctx))
               .thenApply(unused -> createSuccessResponse(ctx)));
      } finally {
         resetParams();
      }
   }

   protected void remove(Channel ch) {
      try {
         OperationContext ctx = createOperationContext();
         sendResponseOrdered(ch, cache.removeAsync(ctx.key)
               .thenApply(prev -> prev == null ? createNotExistResponse(ctx) : createSuccessResponse(ctx)));
      } finally {
         resetParams();
      }
   }

   protected void get(ByteBuf buffer, Channel ch) throws StreamCorruptedException {
      List<byte[]> keys = TextProtocolUtil.extractKeys(buffer);
      int numberOfKeys = keys.size();
      try {
         if (numberOfKeys > 1) {
            Map<byte[], CacheEntry<byte[], byte[]>> map = Collections.synchronizedMap(new LinkedHashMap<>());
            CompletionStage<Void> lastStage = doGetMultipleKeys(checkKeyLength(keys.get(0), true, buffer), map);
            for (int i = 1; i < numberOfKeys; ++i) {
               final byte[] key = checkKeyLength(keys.get(i), true, buffer);
               lastStage = lastStage.thenCompose(unused -> doGetMultipleKeys(key, map));
            }
            sendResponseOrdered(ch, lastStage.thenApply(unused -> createMultiGetResponse(map)));
         } else {
            byte[] key = checkKeyLength(keys.get(0), true, buffer);
            boolean requiresVersion = header.operation == MemcachedOperation.GetWithVersionRequest;
            sendResponseOrdered(ch, cache.getCacheEntryAsync(key).thenApply(entry -> createGetResponse(key, entry, requiresVersion)));
         }
      } finally {
         resetParams();
      }
   }

   private CompletionStage<Void> doGetMultipleKeys(byte[] key, Map<byte[], CacheEntry<byte[], byte[]>> responses) {
      try {
         return cache.getCacheEntryAsync(key)
               .thenAccept(cacheEntry -> {
                  if (cacheEntry != null) {
                     responses.put(key, cacheEntry);
                  }
               });
      } catch (Throwable t) {
         return failedFuture(t);
      }
   }

   private boolean readHeader(ByteBuf buffer, RequestHeader header) throws IOException {
      byteBuffer.reset();
      boolean endOfOp = readElement(buffer, byteBuffer);
      String streamOp = extractString(byteBuffer);
      MemcachedOperation op = toRequest(streamOp, endOfOp, buffer);
      if (op == MemcachedOperation.StatsRequest && !endOfOp) {
         String line = readDiscardedLine(buffer).trim();
         if (!line.isEmpty())
            throw new StreamCorruptedException("Stats command does not accept arguments: " + line);
         else
            endOfOp = true;
      }
      if (op == MemcachedOperation.VerbosityRequest) {
         if (!endOfOp)
            skipLine(buffer); // Read rest of line to clear the operation
         throw new StreamCorruptedException("Memcached 'verbosity' command is unsupported");
      }

      header.operation = op;
      return endOfOp;
   }

   private KeyValuePair<byte[], Boolean> readKey(ByteBuf b) throws IOException {
      byteBuffer.reset();
      boolean endOfOp = readElement(b, byteBuffer);
      byte[] keyBytes = byteBuffer.toByteArray();
      byte[] k = checkKeyLength(keyBytes, endOfOp, b);
      return new KeyValuePair<>(k, endOfOp);
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      Channel ch = ctx.channel();
      // Log it just in case the channel is closed or similar
      log.debug("Exception caught", cause);
      if (!(cause instanceof IOException)) {
         Object errorResponse = createErrorResponse(cause);
         if (errorResponse != null) {
            if (errorResponse instanceof byte[]) {
               ch.writeAndFlush(wrappedBuffer((byte[]) errorResponse), ch.voidPromise());
            } else if (errorResponse instanceof CharSequence) {
               ch.writeAndFlush(Unpooled.copiedBuffer((CharSequence) errorResponse, TextProtocolUtil.CHARSET), ch.voidPromise());
            } else {
               ch.writeAndFlush(errorResponse, ch.voidPromise());
            }
         }
      }
   }

   private static byte[] checkKeyLength(byte[] k, boolean endOfOp, ByteBuf b) throws StreamCorruptedException {
      CharBuffer keyCharBuffer = UTF_8.decode(ByteBuffer.wrap(k));
      if (keyCharBuffer.length() > 250) {
         if (!endOfOp) skipLine(b); // Clear the rest of line
         throw new StreamCorruptedException("Key length over the 250 character limit");
      } else return k;
   }

   private boolean readParameters(ByteBuf b) throws IOException {
      List<String> args = readSplitLine(b);
      boolean endOfOp = false;
      if (args.size() != 0) {
         if (log.isTraceEnabled()) log.tracef("Operation parameters: %s", args);
         try {
            switch (header.operation) {
               case TouchRequest:
                  endOfOp = true;
                  params = readTouchParameters(args);
                  break;
               case RemoveRequest:
                  params = readRemoveParameters(args);
                  break;
               case IncrementRequest:
               case DecrementRequest:
                  endOfOp = true;
                  params = readIncrDecrParameters(args);
                  break;
               case FlushAllRequest:
                  params = readFlushAllParameters(args);
                  break;
               case PutRequest:
               default:
                  params = readStorageParameters(args);
                  break;
            }
         } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException("Missing content in command line " + args);
         }
      }
      if (log.isTraceEnabled()) log.tracef("Operation parameters decoded: %s", params);
      return endOfOp;
   }

   private MemcachedParameters readTouchParameters(List<String> args) throws StreamCorruptedException,
           EOFException {
      int streamLifespan = getLifespan(args.get(0));
      int lifespan = streamLifespan <= 0 ? -1 : getLifespan(args.get(0));
      boolean noReply = parseNoReply(1, args);
      return new MemcachedParameters(0, lifespan, -1, -1, noReply, 0, "", 0);
   }

   private MemcachedParameters readRemoveParameters(List<String> args) throws StreamCorruptedException {
      int delayedDeleteTime = parseDelayedDeleteTime(args);
      boolean noReply = delayedDeleteTime == -1 && parseNoReply(0, args);
      return new MemcachedParameters(-1, -1, -1, -1, noReply, 0, "", 0);
   }

   private MemcachedParameters readIncrDecrParameters(List<String> args) throws StreamCorruptedException {
      String delta = args.get(0);
      return new MemcachedParameters(-1, -1, -1, -1, parseNoReply(1, args), 0, delta, 0);
   }

   private MemcachedParameters readFlushAllParameters(List<String> args) throws StreamCorruptedException {
      boolean noReplyFound = false;
      int flushDelay;
      try {
         flushDelay = friendlyMaxIntCheck(args.get(0), "Flush delay");
      } catch (NumberFormatException n) {
         if (n.getMessage().contains("noreply")) {
            noReplyFound = true;
            flushDelay = 0;
         } else throw n;
      }
      boolean noReply = noReplyFound || parseNoReply(1, args);
      return new MemcachedParameters(-1, -1, -1, -1, noReply, 0, "", flushDelay);
   }

   private MemcachedParameters readStorageParameters(List<String> args) throws StreamCorruptedException,
         EOFException {
      int index = 0;
      long flags = getFlags(args.get(index));
      if (flags < 0) throw new StreamCorruptedException("Flags cannot be negative: " + flags);
      index += 1;
      int streamLifespan = getLifespan(args.get(index));
      int lifespan = streamLifespan <= 0 ? -1 : getLifespan(args.get(index));
      index += 1;
      int length = getLength(args.get(index));
      if (length < 0) throw new StreamCorruptedException("Negative bytes length provided: " + length);
      long streamVersion;
      if (header.operation == MemcachedOperation.ReplaceIfUnmodifiedRequest) {
         index += 1;
         streamVersion = getVersion(args.get(index));
      } else {
         streamVersion = 1;
      }
      index += 1;
      boolean noReply = parseNoReply(index, args);
      return new MemcachedParameters(length, lifespan, -1, streamVersion, noReply, flags, "", 0);
   }

   private EntryVersion generateVersion() {
      ComponentRegistry registry = getCacheRegistry();
      VersionGenerator cacheVersionGenerator = registry.getComponent(VersionGenerator.class);
      if (cacheVersionGenerator == null) {
         NumericVersionGenerator newVersionGenerator = new NumericVersionGenerator();
         registry.registerComponent(newVersionGenerator, VersionGenerator.class);
         return newVersionGenerator.generateNew();
      } else {
         return cacheVersionGenerator.generateNew();
      }
   }


   private void readValue(ByteBuf b) {
      b.readBytes(rawValue);
      skipLine(b); // read the rest of line to clear CRLF after value Byte[]
   }

   private long getFlags(String flags) throws EOFException {
      if (flags == null) throw new EOFException("No flags passed");
      try {
         return numericLimitCheck(flags, 4294967295L, "Flags");
      } catch (NumberFormatException n) {
         return numericLimitCheck(flags, 4294967295L, "Flags", n);
      }
   }

   private int getLifespan(String lifespan) throws EOFException {
      if (lifespan == null) throw new EOFException("No expiry passed");
      return friendlyMaxIntCheck(lifespan, "Lifespan");
   }

   private int getLength(String length) throws EOFException {
      if (length == null) throw new EOFException("No bytes passed");
      return friendlyMaxIntCheck(length, "The number of bytes");
   }

   private long getVersion(String version) throws EOFException {
      if (version == null) throw new EOFException("No cas passed");
      return Long.parseLong(version);
   }

   private boolean parseNoReply(int expectedIndex, List<String> args) throws StreamCorruptedException {
      if (args.size() > expectedIndex) {
         if ("noreply".equals(args.get(expectedIndex)))
            return true;
         else
            throw new StreamCorruptedException("Unable to parse noreply optional argument");
      } else return false;
   }

   private int parseDelayedDeleteTime(List<String> args) {
      if (args.size() > 0) {
         try {
            return Integer.parseInt(args.get(0));
         } catch (NumberFormatException e) {
            return -1; // Either unformatted number, or noreply found
         }
      } else return 0;
   }

   private Configuration getCacheConfiguration() {
      return cache.getCacheConfiguration();
   }

   private ComponentRegistry getCacheRegistry() {
      return cache.getComponentRegistry();
   }

   private void customDecodeHeader(ChannelHandlerContext ctx, ByteBuf buffer) throws IOException {
      Channel ch = ctx.channel();
      switch (header.operation) {
         case FlushAllRequest:
            flushAll(buffer, ch, false); // Without params
            break;
         case VersionRequest:
            StringBuilder ret = new StringBuilder().append("VERSION ").append(Version.getVersion()).append(CRLF);
            sendResponseOrdered(ch, CompletableFuture.completedFuture(ret));
            break;
         case QuitRequest:
            ch.close();
            break;
         default:
            throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
      }
   }

   protected void customDecodeKey(ChannelHandlerContext ctx, ByteBuf buffer) throws IOException {
      switch (header.operation) {
         case AppendRequest:
         case PrependRequest:
         case IncrementRequest:
         case DecrementRequest:
            key = readKey(buffer).getKey();
            checkpoint(MemcachedDecoderState.DECODE_PARAMETERS);
            break;
         case FlushAllRequest:
            flushAll(buffer, ctx.channel(), true); // With params
            break;
         default:
            throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
      }
   }

   protected void customDecodeValue(ChannelHandlerContext ctx, ByteBuf buffer) {
      Channel ch = ctx.channel();
      switch (header.operation) {
         case AppendRequest:
         case PrependRequest:
            readValue(buffer);
            prependOrAppendData(ch);
            break;
         case IncrementRequest:
         case DecrementRequest:
            incrDecr(ch);
            break;
         default:
            throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
      }
   }

   private void prependOrAppendData(Channel ch) {
      try {
         OperationContext ctx = createOperationContext();
         sendResponseOrdered(ch, cache.getAsync(ctx.key)
               .thenCompose(prev -> {
                  if (prev == null) {
                     return completedFuture(ctx.isNoReply() ? null : NOT_STORED);
                  }
                  byte[] concatenated = ctx.isOperation(MemcachedOperation.AppendRequest) ?
                        concat(prev, ctx.value) :
                        concat(ctx.value, prev);
                  return cache.replaceAsync(ctx.key, prev, concatenated, buildMetadata(ctx))
                        .thenApply(replaced ->
                              replaced ?
                                    (ctx.isNoReply() ? null : STORED) :
                                    (ctx.isNoReply() ? null : NOT_STORED));
               }));
      } finally {
         resetParams();
      }
   }

   private void incrDecr(Channel ch) {
      try {
         OperationContext ctx = createOperationContext();
         sendResponseOrdered(ch, cache.getAsync(ctx.key)
               .thenCompose(prev -> {
                  boolean isIncrement = ctx.isOperation(MemcachedOperation.IncrementRequest);
                  if (prev == null) {
                     if (isStatsEnabled) {
                        if (isIncrement) {
                           incrMisses.incrementAndGet();
                        } else {
                           decrMisses.incrementAndGet();
                        }
                     }
                     return completedFuture(ctx.isNoReply() ? null : NOT_FOUND);
                  }
                  BigInteger prevCounter = new BigInteger(new String(prev));
                  BigInteger delta = validateDelta(ctx.parameters.delta);
                  BigInteger candidateCounter;
                  if (isIncrement) {
                     candidateCounter = prevCounter.add(delta);
                     candidateCounter = candidateCounter.compareTo(MAX_UNSIGNED_LONG) > 0 ? MIN_UNSIGNED : candidateCounter;
                  } else {
                     candidateCounter = prevCounter.subtract(delta);
                     candidateCounter = candidateCounter.compareTo(MIN_UNSIGNED) < 0 ? MIN_UNSIGNED : candidateCounter;
                  }
                  String counterString = candidateCounter.toString();
                  return cache.replaceAsync(ctx.key, prev, counterString.getBytes(), buildMetadata(ctx))
                        .thenApply(replaced -> {
                           if (!replaced) {
                              // If there's a concurrent modification on this key, the spec does not say what to do, so treat it as exceptional
                              throw asCompletionException(new CacheException("Value modified since we retrieved from the cache, old value was " + prevCounter));
                           }
                           if (isStatsEnabled) {
                              if (isIncrement) {
                                 incrHits.incrementAndGet();
                              } else {
                                 decrHits.incrementAndGet();
                              }
                           }
                           return ctx.isNoReply() ? null : counterString + CRLF;
                        });
               }));
      } finally {
         resetParams();
      }
   }

   private void flushAll(ByteBuf b, Channel ch, boolean isReadParams) throws IOException {
      if (isReadParams) readParameters(b);
      try {
         int flushDelay = params == null ? 0 : params.flushDelay;
         Object response = params != null && params.noReply ? null : OK;
         if (flushDelay == 0) {
            sendResponseOrdered(ch, cache.clearAsync().thenApply(unused -> response));
            return;
         }
         scheduler.schedule(cache::clear, toMillis(flushDelay), TimeUnit.MILLISECONDS);
         sendResponseOrdered(ch, CompletableFuture.completedFuture(response));
      } finally {
         resetParams();
      }
   }

   private BigInteger validateDelta(String delta) {
      BigInteger bigIntDelta = new BigInteger(delta);
      if (bigIntDelta.compareTo(MAX_UNSIGNED_LONG) > 0)
         throw asCompletionException(new StreamCorruptedException("Increment or decrement delta sent (" + delta + ") exceeds unsigned limit ("
               + MAX_UNSIGNED_LONG + ")"));
      else if (bigIntDelta.compareTo(MIN_UNSIGNED) < 0)
         throw asCompletionException(new StreamCorruptedException("Increment or decrement delta cannot be negative: " + delta));
      return bigIntDelta;
   }

   private Object createSuccessResponse(OperationContext context) {
      if (isStatsEnabled && context.isOperation(MemcachedOperation.ReplaceIfUnmodifiedRequest)) {
         replaceIfUnmodifiedHits.incrementAndGet();
      }
      if (context.isNoReply()) {
         return null;
      }
      if (context.isOperation(MemcachedOperation.RemoveRequest)) {
         return DELETED;
      } else {
         return STORED;
      }
   }

   Object createNotExecutedResponse(OperationContext context) {
      if (isStatsEnabled && context.isOperation(MemcachedOperation.ReplaceIfUnmodifiedRequest)) {
         replaceIfUnmodifiedBadval.incrementAndGet();
      }
      if (context.isNoReply()) {
         return null;
      }
      if (context.isOperation(MemcachedOperation.ReplaceIfUnmodifiedRequest)) {
         return EXISTS;
      } else {
         return NOT_STORED;
      }
   }

   Object createNotExistResponse(OperationContext context) {
      if (isStatsEnabled && context.isOperation(MemcachedOperation.ReplaceIfUnmodifiedRequest)) {
            replaceIfUnmodifiedMisses.incrementAndGet();
      }
      return context.isNoReply() ? null : NOT_FOUND;
   }

   private static Object createGetResponse(byte[] k, CacheEntry<byte[], byte[]> entry, boolean requiresVersions) {
      if (entry == null) {
         return END;
      }
      return requiresVersions ? buildSingleGetWithVersionResponse(k, entry) : buildSingleGetResponse(k, entry);
   }

   private static Object createTouchedResponse(OperationContext context) {
      return context.isNoReply() ? null : TOUCHED;
   }

   private static ByteBuf buildSingleGetResponse(byte[] k, CacheEntry<byte[], byte[]> entry) {
      ByteBuf buf = buildGetHeaderBegin(k, entry, END_SIZE);
      writeGetHeaderData(entry.getValue(), buf);
      return writeGetHeaderEnd(buf);
   }

   private static Object createMultiGetResponse(Map<byte[], CacheEntry<byte[], byte[]>> pairs) {
      Stream.Builder<ByteBuf> elements = Stream.builder();
      pairs.forEach((k, v) -> elements.add(buildGetResponse(k, v)));
      elements.add(wrappedBuffer(END));
      return elements.build().toArray(ByteBuf[]::new);
   }

   void handleModification(Channel ch, ByteBuf buf) throws IOException {
      KeyValuePair<byte[], Boolean> pair = readKey(buf);
      key = pair.getKey();
      if (pair.getValue()) {
         // If it's the end of the operation, it can only be a remove
         remove(ch);
      } else {
         checkpoint(MemcachedDecoderState.DECODE_PARAMETERS);
      }
   }

   private void resetParams() {
      if (log.isTraceEnabled()) {
         log.tracef("Resetting parameters using instance @%x", System.identityHashCode(this));
      }
      checkpoint(MemcachedDecoderState.DECODE_HEADER);
      // Reset parameters to avoid leaking previous params
      // into a request that has no params
      params = null;
      rawValue = null; // Clear reference to value
      key = null;
   }

   private Object createErrorResponse(Throwable t) {
      StringBuilder sb = new StringBuilder();
      if (t instanceof MemcachedException) {
         Throwable cause = t.getCause();
         if (cause instanceof UnknownOperationException) {
            log.exceptionReported(cause);
            return ERROR;
         } else if (cause instanceof ClosedChannelException) {
            log.exceptionReported(cause);
            return null; // no-op, only log
         } else if (cause instanceof IOException || cause instanceof NumberFormatException ||
               cause instanceof IllegalStateException) {
            return logAndCreateErrorMessage(sb, (MemcachedException) t);
         } else {
            return sb.append(t.getMessage()).append(CRLF);
         }
      } else if (t instanceof ClosedChannelException) {
         log.exceptionReported(t);
         return null; // no-op, only log
      } else {
         return sb.append(SERVER_ERROR).append(t.getMessage()).append(CRLF);
      }
   }

   private Metadata buildMetadata(OperationContext context) {
      return new MemcachedMetadata.Builder()
            .flags(context.parameters.flags)
            .version(generateVersion())
            .lifespan(context.parameters.lifespan > 0 ? toMillis(context.parameters.lifespan) : -1)
            .build();
   }

   private Metadata touchMetadata(OperationContext context, CacheEntry<?,?> entry) {
      return new MemcachedMetadata.Builder()
            .merge(entry.getMetadata())
            .lifespan(context.parameters.lifespan > 0 ? toMillis(context.parameters.lifespan) : -1)
            .build();
   }

   private StringBuilder logAndCreateErrorMessage(StringBuilder sb, MemcachedException m) {
      log.exceptionReported(m.getCause());
      return sb.append(m.getMessage()).append(CRLF);
   }

   /**
    * Transforms lifespan pass as seconds into milliseconds
    * following this rule:
    * <p>
    * If lifespan is bigger than number of seconds in 30 days,
    * then it is considered unix time. After converting it to
    * milliseconds, we substract the current time in and the
    * result is returned.
    * <p>
    * Otherwise it's just considered number of seconds from
    * now and it's returned in milliseconds unit.
    */
   private long toMillis(int lifespan) {
      if (lifespan > SecondsInAMonth) {
         long unixTimeExpiry = TimeUnit.SECONDS.toMillis(lifespan) - timeService.wallClockTime();
         return unixTimeExpiry < 0 ? 0 : unixTimeExpiry;
      } else {
         return TimeUnit.SECONDS.toMillis(lifespan);
      }
   }

   protected void writeResponseOrThrowable(Channel ch, Object response, Throwable throwable) {
      if (throwable != null) {
         Throwable cause = CompletableFutures.extractException(throwable);
         if (cause instanceof IOException || cause instanceof NumberFormatException) {
            cause = wrapBadFormat(cause);
         } else {
            cause = wrapServerError(cause);
         }
         ch.pipeline().fireExceptionCaught(cause);
      } else {
         writeResponse(ch, response);
      }
   }

   private void writeResponse(Channel ch, Object response) {
      if (response != null) {
         if (log.isTraceEnabled())
            log.tracef("Write response using instance @%x: %s", System.identityHashCode(this), Util.toStr(response));
         if (response instanceof ByteBuf[]) {
            for (ByteBuf buf : (ByteBuf[]) response) {
               ch.write(buf, ch.voidPromise());
               ch.flush();
            }
         } else if (response instanceof byte[]) {
            ch.writeAndFlush(wrappedBuffer((byte[]) response), ch.voidPromise());
         } else if (response instanceof CharSequence) {
            ch.writeAndFlush(Unpooled.copiedBuffer((CharSequence) response, CharsetUtil.UTF_8), ch.voidPromise());
         } else {
            ch.writeAndFlush(response, ch.voidPromise());
         }
      }
   }

   Object createStatsResponse() {
      Stats stats = cache.getAdvancedCache().getStats();
      StringBuilder sb = new StringBuilder();
      return new ByteBuf[]{
            buildStat("pid", 0, sb),
            buildStat("uptime", stats.getTimeSinceStart(), sb),
            buildStat("time", TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime()), sb),
            buildStat("version", cache.getVersion(), sb),
            buildStat("pointer_size", 0, sb), // Unsupported
            buildStat("rusage_user", 0, sb), // Unsupported
            buildStat("rusage_system", 0, sb), // Unsupported
            buildStat("curr_items", stats.getApproximateEntries(), sb),
            buildStat("total_items", stats.getStores(), sb),
            buildStat("bytes", 0, sb), // Unsupported
            buildStat("curr_connections", 0, sb), // TODO: Through netty?
            buildStat("total_connections", 0, sb), // TODO: Through netty?
            buildStat("connection_structures", 0, sb), // Unsupported
            buildStat("cmd_get", stats.getRetrievals(), sb),
            buildStat("cmd_set", stats.getStores(), sb),
            buildStat("get_hits", stats.getHits(), sb),
            buildStat("get_misses", stats.getMisses(), sb),
            buildStat("delete_misses", stats.getRemoveMisses(), sb),
            buildStat("delete_hits", stats.getRemoveHits(), sb),
            buildStat("incr_misses", incrMisses, sb),
            buildStat("incr_hits", incrHits, sb),
            buildStat("decr_misses", decrMisses, sb),
            buildStat("decr_hits", decrHits, sb),
            buildStat("cas_misses", replaceIfUnmodifiedMisses, sb),
            buildStat("cas_hits", replaceIfUnmodifiedHits, sb),
            buildStat("cas_badval", replaceIfUnmodifiedBadval, sb),
            buildStat("auth_cmds", 0, sb), // Unsupported
            buildStat("auth_errors", 0, sb), // Unsupported
            //TODO: Evictions are measure by evict calls, but not by nodes are that are expired after the entry's lifespan has expired.
            buildStat("evictions", stats.getEvictions(), sb),
            buildStat("bytes_read", transport.getTotalBytesRead(), sb),
            buildStat("bytes_written", transport.getTotalBytesWritten(), sb),
            buildStat("limit_maxbytes", 0, sb), // Unsupported
            buildStat("threads", 0, sb), // TODO: Through netty?
            buildStat("conn_yields", 0, sb), // Unsupported
            buildStat("reclaimed", 0, sb), // Unsupported
            wrappedBuffer(END)
      };
   }

   private ByteBuf buildStat(String stat, Object value, StringBuilder sb) {
      sb.append("STAT").append(' ').append(stat).append(' ').append(value).append(CRLF);
      ByteBuf buffer = wrappedBuffer(sb.toString().getBytes());
      sb.setLength(0);
      return buffer;
   }

   private ByteBuf buildStat(String stat, int value, StringBuilder sb) {
      sb.append("STAT").append(' ').append(stat).append(' ').append(value).append(CRLF);
      ByteBuf buffer = wrappedBuffer(sb.toString().getBytes());
      sb.setLength(0);
      return buffer;
   }

   private ByteBuf buildStat(String stat, long value, StringBuilder sb) {
      sb.append("STAT").append(' ').append(stat).append(' ').append(value).append(CRLF);
      ByteBuf buffer = wrappedBuffer(sb.toString().getBytes());
      sb.setLength(0);
      return buffer;
   }

   private static ByteBuf buildGetResponse(byte[] k, CacheEntry<byte[], byte[]> entry) {
      ByteBuf buf = buildGetHeaderBegin(k, entry, 0);
      return writeGetHeaderData(entry.getValue(), buf);
   }

   private static ByteBuf buildGetHeaderBegin(byte[] key, CacheEntry<byte[], byte[]> entry,
                                       int extraSpace) {
      byte[] data = entry.getValue();
      byte[] dataSize = String.valueOf(data.length).getBytes();

      byte[] flags;
      Metadata metadata = entry.getMetadata();
      if (metadata instanceof MemcachedMetadata) {
         long metaFlags = ((MemcachedMetadata) metadata).flags;
         flags = String.valueOf(metaFlags).getBytes();
      } else {
         flags = ZERO;
      }

      int flagsSize = flags.length;
      ByteBuf buf = buffer(VALUE_SIZE + key.length + data.length + flagsSize + dataSize.length + 6 + extraSpace);
      buf.writeBytes(VALUE);
      buf.writeBytes(key);
      buf.writeByte(SP);
      buf.writeBytes(flags);
      buf.writeByte(SP);
      buf.writeBytes(dataSize);
      return buf;
   }

   private static ByteBuf writeGetHeaderData(byte[] data, ByteBuf buf) {
      buf.writeBytes(CRLFBytes);
      buf.writeBytes(data);
      buf.writeBytes(CRLFBytes);
      return buf;
   }

   private static ByteBuf writeGetHeaderEnd(ByteBuf buf) {
      buf.writeBytes(END);
      return buf;
   }

   private static ByteBuf buildSingleGetWithVersionResponse(byte[] k, CacheEntry<byte[], byte[]> entry) {
      byte[] v = entry.getValue();
      // TODO: Would be nice for EntryVersion to allow retrieving the version itself...
      byte[] version = String.valueOf(((NumericVersion) entry.getMetadata().version()).getVersion()).getBytes();
      ByteBuf buf = buildGetHeaderBegin(k, entry, version.length + 1 + END_SIZE);
      buf.writeByte(SP); // 1
      buf.writeBytes(version); // version.length
      writeGetHeaderData(v, buf);
      return writeGetHeaderEnd(buf);
   }

   private int friendlyMaxIntCheck(String number, String message) {
      try {
         return Integer.parseInt(number);
      } catch (NumberFormatException e) {
         return numericLimitCheck(number, Integer.MAX_VALUE, message, e);
      }
   }

   private int numericLimitCheck(String number, long maxValue, String message, NumberFormatException n) {
      if (Long.parseLong(number) > maxValue)
         throw new NumberFormatException(message + " sent (" + number
               + ") exceeds the limit (" + maxValue + ")");
      else throw n;
   }

   private long numericLimitCheck(String number, long maxValue, String message) {
      long numeric = Long.parseLong(number);
      if (numeric > maxValue)
         throw new NumberFormatException(message + " sent (" + number
               + ") exceeds the limit (" + maxValue + ")");
      return numeric;
   }

   private MemcachedOperation toRequest(String commandName, Boolean endOfOp, ByteBuf buffer) throws UnknownOperationException {
      if (log.isTraceEnabled()) log.tracef("Operation: '%s'", commandName);
      switch (commandName) {
         case "get":
            return MemcachedOperation.GetRequest;
         case "set":
            return MemcachedOperation.PutRequest;
         case "touch":
            return MemcachedOperation.TouchRequest;
         case "add":
            return MemcachedOperation.PutIfAbsentRequest;
         case "delete":
            return MemcachedOperation.RemoveRequest;
         case "replace":
            return MemcachedOperation.ReplaceRequest;
         case "cas":
            return MemcachedOperation.ReplaceIfUnmodifiedRequest;
         case "append":
            return MemcachedOperation.AppendRequest;
         case "prepend":
            return MemcachedOperation.PrependRequest;
         case "gets":
            return MemcachedOperation.GetWithVersionRequest;
         case "incr":
            return MemcachedOperation.IncrementRequest;
         case "decr":
            return MemcachedOperation.DecrementRequest;
         case "flush_all":
            return MemcachedOperation.FlushAllRequest;
         case "version":
            return MemcachedOperation.VersionRequest;
         case "stats":
            return MemcachedOperation.StatsRequest;
         case "verbosity":
            return MemcachedOperation.VerbosityRequest;
         case "quit":
            return MemcachedOperation.QuitRequest;
         default:
            if (!endOfOp) {
               String line = readDiscardedLine(buffer); // Read rest of line to clear the operation
               log.debugf("Unexpected operation '%s', rest of line contains: %s", commandName, line);
            }
            throw new UnknownOperationException("Unknown operation: " + commandName);
      }
   }

   private OperationContext createOperationContext() {
      return new OperationContext(header, params, key, rawValue);
   }

   private void sendResponseOrdered(Channel ch, CompletionStage<Object> rsp) {
      new ResponseEntry(this, ch).queueResponse(rsp);
   }
}

class MemcachedParameters {
   final int valueLength;
   final int lifespan;
   final int maxIdle;
   final long streamVersion;
   final boolean noReply;
   final long flags;
   final String delta;
   final int flushDelay;

   MemcachedParameters(int valueLength, int lifespan, int maxIdle, long streamVersion, boolean noReply, long flags,
                       String delta, int flushDelay) {
      this.valueLength = valueLength;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.streamVersion = streamVersion;
      this.noReply = noReply;
      this.flags = flags;
      this.delta = delta;
      this.flushDelay = flushDelay;
   }

   @Override
   public String toString() {
      return "MemcachedParameters{" +
            "valueLength=" + valueLength +
            ", lifespan=" + lifespan +
            ", maxIdle=" + maxIdle +
            ", streamVersion=" + streamVersion +
            ", noReply=" + noReply +
            ", flags=" + flags +
            ", delta='" + delta + '\'' +
            ", flushDelay=" + flushDelay +
            '}';
   }
}

class MemcachedException extends Exception {
   MemcachedException(String message, Throwable cause) {
      super(message, cause);
   }
}

class RequestHeader {
   MemcachedOperation operation;

   @Override
   public String toString() {
      return "RequestHeader{" +
            "operation=" + operation +
            '}';
   }
}

class UnknownOperationException extends StreamCorruptedException {
   UnknownOperationException(String reason) {
      super(reason);
   }
}

class CacheUnavailableException extends Exception {
   CacheUnavailableException(String msg) {
      super(msg);
   }
}

class OperationContext {
   final RequestHeader header;
   final MemcachedParameters parameters;
   final byte[] key;
   final byte[] value;

   OperationContext(RequestHeader header, MemcachedParameters parameters, byte[] key, byte[] value) {
      this.header = Objects.requireNonNull(header);
      this.parameters = parameters;
      this.key = key;
      this.value = value;
   }

   boolean isNoReply() {
      return parameters != null && parameters.noReply;
   }

   boolean isOperation(MemcachedOperation operation) {
      return header.operation == operation;
   }
}

class ResponseEntry implements BiConsumer<Object, Throwable>, Runnable {
   volatile Object response;
   volatile Throwable throwable;
   CompletionStage<Void> responseSent;
   final MemcachedDecoder decoder;
   final Channel ch;

   ResponseEntry(MemcachedDecoder decoder, Channel ch) {
      this.decoder = decoder;
      this.ch = ch;
   }

   void queueResponse(CompletionStage<Object> operationResponse) {
      assert ch.eventLoop().inEventLoop();
      AggregateCompletionStage<Void> all = CompletionStages.aggregateCompletionStage();
      if (decoder.lastResponse != null) {
         all.dependsOn(decoder.lastResponse.responseSent);
      }
      all.dependsOn(operationResponse.whenComplete(this));
      responseSent = all.freeze()
            .exceptionally(CompletableFutures.toNullFunction())
            .thenRunAsync(this, ch.eventLoop());
      decoder.lastResponse = this;
   }

   @Override
   public void accept(Object response, Throwable throwable) {
      // store the response
      this.response = response;
      this.throwable = throwable;
   }

   @Override
   public void run() {
      // send the response, fields are populated!
      decoder.writeResponseOrThrowable(ch, response, throwable);
   }
}
