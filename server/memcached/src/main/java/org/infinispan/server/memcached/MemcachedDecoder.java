package org.infinispan.server.memcached;

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
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.JavaCompatEncoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.memcached.logging.JavaLog;
import org.infinispan.stats.Stats;
import org.infinispan.util.KeyValuePair;

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

   public MemcachedDecoder(AdvancedCache<String, byte[]> memcachedCache, ScheduledExecutorService scheduler,
                           NettyTransport transport, Predicate<? super String> ignoreCache) {
      super(MemcachedDecoderState.DECODE_HEADER);
      CompatibilityModeConfiguration compatibility = memcachedCache.getCacheConfiguration().compatibility();
      boolean compat = compatibility.enabled();
      AdvancedCache<?, ?> c = memcachedCache.getAdvancedCache();
      if (compat) {
         boolean hasCompatMarshaller = compatibility.marshaller() != null;
         Class<? extends Encoder> valueEncoder = hasCompatMarshaller ? CompatModeEncoder.class : JavaCompatEncoder.class;
         c = c.withEncoding(IdentityEncoder.class, valueEncoder);
      }
      cache = (AdvancedCache<String, byte[]>) c;
      this.scheduler = scheduler;
      this.transport = transport;
      this.ignoreCache = ignoreCache;
      isStatsEnabled = cache.getCacheConfiguration().jmxStatistics().enabled();
   }

   private final AdvancedCache<String, byte[]> cache;
   private final ScheduledExecutorService scheduler;
   protected final NettyTransport transport;
   protected final Predicate<? super String> ignoreCache;

   private final static JavaLog log = LogFactory.getLog(MemcachedDecoder.class, JavaLog.class);
   private final static boolean isTrace = log.isTraceEnabled();

   private static final int SecondsInAMonth = 60 * 60 * 24 * 30;

   long defaultLifespanTime;
   long defaultMaxIdleTime;

   protected String key;
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
   private ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
   protected RequestHeader header;

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      try {
         decodeDispatch(ctx, in, out);
      } finally {
         // reset in all cases
         byteBuffer.reset();
      }
   }

   protected Object replace() {
      // Avoid listener notification for a simple optimization
      // on whether a new version should be calculated or not.
      Object prev = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).get(key);
      if (prev != null) {
         // Generate new version only if key present
         prev = cache.replace(key, createValue(), buildMetadata());
      }
      if (prev != null)
         return createSuccessResponse();
      else
         return createNotExecutedResponse();
   }

   protected Object replaceIfUnmodified() {
      CacheEntry<String, byte[]> entry = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntry(key);
      if (entry != null) {
         byte[] prev = entry.getValue();
         NumericVersion streamVersion = new NumericVersion(params.streamVersion);
         if (entry.getMetadata().version().equals(streamVersion)) {
            byte[] v = createValue();
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            boolean replaced = cache.replace(key, prev, v, buildMetadata());
            if (replaced)
               return createSuccessResponse();
            else
               return createNotExecutedResponse();
         } else {
            return createNotExecutedResponse();
         }
      } else return createNotExistResponse();
   }

   private void decodeDispatch(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws MemcachedException {
      try {
         if (isTrace) // To aid debugging
            log.tracef("Decode using instance @%x", System.identityHashCode(this));
         MemcachedDecoderState state = state();
         switch (state) {
            case DECODE_HEADER:
               decodeHeader(ctx, in, state, out);
               break;
            case DECODE_KEY:
               decodeKey(ctx, in);
               break;
            case DECODE_PARAMETERS:
               decodeParameters(ctx, in, state);
               break;
            case DECODE_VALUE:
               decodeValue(ctx, in, state);
               break;
         }
      } catch (IOException | NumberFormatException e) {
         ctx.pipeline().fireExceptionCaught(new MemcachedException(CLIENT_ERROR_BAD_FORMAT + e.getMessage(), e));
      } catch (Exception e) {
         throw new MemcachedException(SERVER_ERROR + e, e);
      }
   }

   void decodeHeader(ChannelHandlerContext ctx, ByteBuf buffer, MemcachedDecoderState state, List<Object> out)
         throws CacheUnavailableException, IOException {
      header = new RequestHeader();
      Optional<Boolean> endOfOp = readHeader(buffer, header);
      if (!endOfOp.isPresent()) {
         // Something went wrong reading the header, so get more bytes.
         // It can happen with Hot Rod if the header is completely corrupted
         return;
      }
      Channel ch = ctx.channel();
      String cacheName = cache.getName();
      if (ignoreCache.test(cacheName)) throw new CacheUnavailableException(cacheName);
      cacheConfiguration = getCacheConfiguration();
      defaultLifespanTime = cacheConfiguration.expiration().lifespan();
      defaultMaxIdleTime = cacheConfiguration.expiration().maxIdle();
      if (endOfOp.get()) {
         Object message;
         switch (header.operation) {
            case StatsRequest:
               message = writeResponse(ch, createStatsResponse());
               break;
            default:
               customDecodeHeader(ctx, buffer);
               message = null;
               break;
         }
         if (message instanceof PartialResponse) {
            out.add(((PartialResponse) message).buffer);
         }
      } else {
         checkpoint(MemcachedDecoderState.DECODE_KEY);
      }
   }

   void decodeKey(ChannelHandlerContext ctx, ByteBuf buffer) throws IOException {
      Channel ch = ctx.channel();
      switch (header.operation) {
         case GetRequest:
         case GetWithVersionRequest:
            writeResponse(ch, get(buffer));
            break;
         case PutRequest:
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

   private void decodeParameters(ChannelHandlerContext ctx, ByteBuf buffer, MemcachedDecoderState state) throws IOException {
      Channel ch = ctx.channel();
      boolean endOfOp = readParameters(ch, buffer);
      if (!endOfOp && params.valueLength > 0) {
         // Create value holder and checkpoint only if there's more to read
         rawValue = new byte[params.valueLength];
         checkpoint(MemcachedDecoderState.DECODE_VALUE);
      } else if (params.valueLength == 0) {
         rawValue = new byte[0];
         decodeValue(ctx, buffer, state);
      } else {
         decodeValue(ctx, buffer, state);
      }
   }

   private void decodeValue(ChannelHandlerContext ctx, ByteBuf buffer, MemcachedDecoderState state)
         throws StreamCorruptedException {
      Channel ch = ctx.channel();
      Object ret;
      switch (header.operation) {
         case PutRequest:
            readValue(buffer);
            ret = put();
            break;
         case PutIfAbsentRequest:
            readValue(buffer);
            ret = putIfAbsent();
            break;
         case ReplaceRequest:
            readValue(buffer);
            ret = replace();
            break;
         case ReplaceIfUnmodifiedRequest:
            readValue(buffer);
            ret = replaceIfUnmodified();
            break;
         case RemoveRequest:
            ret = remove();
            break;
         default:
            customDecodeValue(ctx, buffer);
            ret = null;
      }
      writeResponse(ch, ret);
   }


   private Object putIfAbsent() {
      Object prev = cache.get(key);
      if (prev == null) {
         // Generate new version only if key not present
         prev = cache.putIfAbsent(key, createValue(), buildMetadata());
      }
      if (prev == null)
         return createSuccessResponse();
      else
         return createNotExecutedResponse();
   }

   private Object put() {
      // Get an optimised cache in case we can make the operation more efficient
      Object prev = cache.put(key, createValue(), buildMetadata());
      return createSuccessResponse();
   }


   private Optional<Boolean> readHeader(ByteBuf buffer, RequestHeader header) throws IOException {
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
      return Optional.of(endOfOp);
   }

   KeyValuePair<String, Boolean> readKey(ByteBuf b) throws IOException {
      boolean endOfOp = readElement(b, byteBuffer);
      String k = extractString(byteBuffer);
      checkKeyLength(k, endOfOp, b);
      return new KeyValuePair<>(k, endOfOp);
   }

   private List<String> readKeys(ByteBuf b) {
      return readSplitLine(b);
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
      // After writing back an error, reset params and revert to initial state
      resetParams();
   }

   protected Object get(ByteBuf buffer) throws StreamCorruptedException {
      List<String> keys = readKeys(buffer);
      if (keys.size() > 1) {
         Map<String, CacheEntry<String, byte[]>> map = new HashMap<>();
         for (String key : keys) {
            CacheEntry<String, byte[]> entry = cache.getCacheEntry(checkKeyLength(key, true, buffer));
            if (entry != null) {
               map.put(key, entry);
            }
         }
         return createMultiGetResponse(map);
      } else {
         String key = checkKeyLength(keys.get(0), true, buffer);
         CacheEntry<String, byte[]> entry = cache.getCacheEntry(key);
         return createGetResponse(key, entry);
      }
   }

   private String checkKeyLength(String k, boolean endOfOp, ByteBuf b) throws StreamCorruptedException {
      if (k.length() > 250) {
         if (!endOfOp) skipLine(b); // Clear the rest of line
         throw new StreamCorruptedException("Key length over the 250 character limit");
      } else return k;
   }

   private boolean readParameters(Channel ch, ByteBuf b) throws IOException {
      List<String> args = readSplitLine(b);
      boolean endOfOp = false;
      if (args.size() != 0) {
         if (isTrace) log.tracef("Operation parameters: %s", args);
         try {
            switch (header.operation) {
               case PutRequest:
                  params = readStorageParameters(args, b);
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
               default:
                  params = readStorageParameters(args, b);
                  break;
            }
         } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException("Missing content in command line " + args);
         }
      }
      return endOfOp;
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

   private MemcachedParameters readStorageParameters(List<String> args, ByteBuf b) throws StreamCorruptedException,
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

   private EntryVersion generateVersion(Cache<String, byte[]> cache) {
      ComponentRegistry registry = getCacheRegistry();
      VersionGenerator cacheVersionGenerator = registry.getComponent(VersionGenerator.class);
      if (cacheVersionGenerator == null) {
         // It could be null, for example when not running in compatibility mode.
         // The reason for that is that if no other component depends on the
         // version generator, the factory does not get invoked.
         NumericVersionGenerator newVersionGenerator = new NumericVersionGenerator()
               .clustered(registry.getComponent(RpcManager.class) != null);
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

   private byte[] createValue() {
      return rawValue;
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
            writeResponse(ch, ret);
            break;
         case QuitRequest:
            ch.close();
            break;
         default:
            throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
      }
   }

   protected void customDecodeKey(ChannelHandlerContext ctx, ByteBuf buffer) throws IOException {
      Channel ch = ctx.channel();
      switch (header.operation) {
         case AppendRequest:
         case PrependRequest:
         case IncrementRequest:
         case DecrementRequest:
            key = readKey(buffer).getKey();
            checkpoint(MemcachedDecoderState.DECODE_PARAMETERS);
            break;
         case FlushAllRequest:
            flushAll(buffer, ch, true); // With params
            break;
         default:
            throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
      }
   }

   protected void customDecodeValue(ChannelHandlerContext ctx, ByteBuf buffer) throws StreamCorruptedException {
      Channel ch = ctx.channel();
      switch (header.operation) {
         case AppendRequest:
         case PrependRequest:
            Object ret;
            readValue(buffer);
            byte[] prev = cache.get(key);
            if (prev != null) {
               byte[] concatenated;
               switch (header.operation) {
                  case AppendRequest:
                     concatenated = concat(prev, rawValue);
                     break;
                  case PrependRequest:
                     concatenated = concat(rawValue, prev);
                     break;
                  default:
                     throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
               }
               if (cache.replace(key, prev, concatenated, buildMetadata())) {
                  ret = !params.noReply ? STORED : null;
               } else {
                  ret = !params.noReply ? NOT_STORED : null;
               }
            } else {
               ret = !params.noReply ? NOT_STORED : null;
            }
            writeResponse(ch, ret);
            break;
         case IncrementRequest:
         case DecrementRequest:
            incrDecr(ch);
            break;
         default:
            throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
      }
   }

   private void incrDecr(Channel ch) throws StreamCorruptedException {
      byte[] prev = cache.get(key);
      Object ret;
      MemcachedOperation op = header.operation;
      if (prev != null) {
         BigInteger prevCounter = new BigInteger(new String(prev));
         BigInteger delta = validateDelta(params.delta);
         BigInteger candidateCounter;
         switch (op) {
            case IncrementRequest:
               candidateCounter = prevCounter.add(delta);
               candidateCounter = candidateCounter.compareTo(MAX_UNSIGNED_LONG) > 0 ? MIN_UNSIGNED : candidateCounter;
               break;
            case DecrementRequest:
               candidateCounter = prevCounter.subtract(delta);
               candidateCounter = candidateCounter.compareTo(MIN_UNSIGNED) < 0 ? MIN_UNSIGNED : candidateCounter;
               break;
            default:
               throw new IllegalArgumentException("Operation " + op + " not supported!");
         }
         String counterString = candidateCounter.toString();
         if (cache.replace(key, prev, counterString.getBytes(), buildMetadata())) {
            if (isStatsEnabled) {
               if (op == MemcachedOperation.IncrementRequest) {
                  incrHits.incrementAndGet();
               } else {
                  decrHits.incrementAndGet();
               }
            }
            ret = !params.noReply ? counterString + CRLF : null;
         } else {
            // If there's a concurrent modification on this key, the spec does not say what to do, so treat it as exceptional
            throw new CacheException("Value modified since we retrieved from the cache, old value was " + prevCounter);
         }
      } else {
         if (isStatsEnabled) {
            if (op == MemcachedOperation.IncrementRequest) {
               incrMisses.incrementAndGet();
            } else {
               decrMisses.incrementAndGet();
            }
         }
         ret = !params.noReply ? NOT_FOUND : null;
      }
      writeResponse(ch, ret);
   }

   private void flushAll(ByteBuf b, Channel ch, boolean isReadParams) throws IOException {
      if (isReadParams) readParameters(ch, b);
      Consumer<Cache<?, ?>> consumer = c -> c.clear();
      int flushDelay = params == null ? 0 : params.flushDelay;
      if (flushDelay == 0)
         consumer.accept(cache);
      else
         scheduler.schedule(() -> consumer.accept(cache), toMillis(flushDelay), TimeUnit.MILLISECONDS);
      Object ret = params == null || !params.noReply ? OK : null;
      writeResponse(ch, ret);
   }

   private BigInteger validateDelta(String delta) throws StreamCorruptedException {
      BigInteger bigIntDelta = new BigInteger(delta);
      if (bigIntDelta.compareTo(MAX_UNSIGNED_LONG) > 0)
         throw new StreamCorruptedException("Increment or decrement delta sent (" + delta + ") exceeds unsigned limit ("
               + MAX_UNSIGNED_LONG + ")");
      else if (bigIntDelta.compareTo(MIN_UNSIGNED) < 0)
         throw new StreamCorruptedException("Increment or decrement delta cannot be negative: " + delta);
      return bigIntDelta;
   }

   private Object createSuccessResponse() {
      if (isStatsEnabled) {
         if (header.operation == MemcachedOperation.ReplaceIfUnmodifiedRequest) {
            replaceIfUnmodifiedHits.incrementAndGet();
         }
      }
      if (params == null || !params.noReply) {
         if (header.operation == MemcachedOperation.RemoveRequest) {
            return DELETED;
         } else {
            return STORED;
         }
      } else return null;
   }

   Object createNotExecutedResponse() {
      if (isStatsEnabled) {
         if (header.operation == MemcachedOperation.ReplaceIfUnmodifiedRequest) {
            replaceIfUnmodifiedBadval.incrementAndGet();
         }
      }
      if (params == null || !params.noReply) {
         if (header.operation == MemcachedOperation.ReplaceIfUnmodifiedRequest) {
            return EXISTS;
         } else {
            return NOT_STORED;
         }
      } else return null;
   }

   Object createNotExistResponse() {
      if (isStatsEnabled) {
         if (header.operation == MemcachedOperation.ReplaceIfUnmodifiedRequest) {
            replaceIfUnmodifiedMisses.incrementAndGet();
         }
      }
      if (params == null || !params.noReply)
         return NOT_FOUND;
      else
         return null;
   }

   Object createGetResponse(String k, CacheEntry<String, byte[]> entry) {
      if (entry != null) {
         switch (header.operation) {
            case GetRequest:
               return buildSingleGetResponse(k, entry);
            case GetWithVersionRequest:
               return buildSingleGetWithVersionResponse(k, entry);
            default:
               throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
         }
      } else
         return END;
   }

   private ByteBuf buildSingleGetResponse(String k, CacheEntry<String, byte[]> entry) {
      ByteBuf buf = buildGetHeaderBegin(k, entry, END_SIZE);
      writeGetHeaderData(entry.getValue(), buf);
      return writeGetHeaderEnd(buf);
   }

   Object createMultiGetResponse(Map<String, CacheEntry<String, byte[]>> pairs) {
      Stream.Builder<ByteBuf> elements = Stream.builder();
      switch (header.operation) {
         case GetRequest:
         case GetWithVersionRequest:
            pairs.forEach((k, v) -> elements.add(buildGetResponse(k, v)));
            elements.add(wrappedBuffer(END));
            return elements.build().toArray(ByteBuf[]::new);
         default:
            throw new IllegalArgumentException("Operation " + header.operation + " not supported!");
      }
   }

   void handleModification(Channel ch, ByteBuf buf) throws IOException {
      KeyValuePair<String, Boolean> pair = readKey(buf);
      key = pair.getKey();
      if (pair.getValue()) {
         // If it's the end of the operation, it can only be a remove
         writeResponse(ch, remove());
      } else {
         checkpoint(MemcachedDecoderState.DECODE_PARAMETERS);
      }
   }

   private void resetParams() {
      checkpoint(MemcachedDecoderState.DECODE_HEADER);
      // Reset parameters to avoid leaking previous params
      // into a request that has no params
      params = null;
      rawValue = null; // Clear reference to value
      key = null;
   }


   protected Object remove() {
      Object prev = cache.remove(key);
      if (prev != null)
         return createSuccessResponse();
      else
         return createNotExistResponse();
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

   protected Metadata buildMetadata() {
      MemcachedMetadataBuilder metadata = new MemcachedMetadataBuilder();
      metadata.version(generateVersion(cache));
      metadata.flags(params.flags);
      if (params.lifespan > 0)
         metadata.lifespan(toMillis(params.lifespan));

      return metadata.build();
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
         long unixTimeExpiry = TimeUnit.SECONDS.toMillis(lifespan) - System.currentTimeMillis();
         return unixTimeExpiry < 0 ? 0 : unixTimeExpiry;
      } else {
         return TimeUnit.SECONDS.toMillis(lifespan);
      }
   }

   protected Object writeResponse(Channel ch, Object response) {
      try {
         if (response != null) {
            if (isTrace) log.tracef("Write response %s", response);
            if (response instanceof ByteBuf[]) {
               for (ByteBuf buf : (ByteBuf[]) response) {
                  ch.write(buf, ch.voidPromise());
                  ch.flush();
               }
            } else if (response instanceof byte[]) {
               ch.writeAndFlush(wrappedBuffer((byte[]) response), ch.voidPromise());
            } else if (response instanceof CharSequence) {
               ch.writeAndFlush(Unpooled.copiedBuffer((CharSequence) response, CharsetUtil.UTF_8), ch.voidPromise());
            } else if (response instanceof PartialResponse) {
               return response;
            } else {
               ch.writeAndFlush(response, ch.voidPromise());
            }
         }
         return null;
      } finally {
         resetParams();
      }
   }

   Object createStatsResponse() {
      Stats stats = cache.getAdvancedCache().getStats();
      StringBuilder sb = new StringBuilder();
      return new ByteBuf[]{
            buildStat("pid", 0, sb),
            buildStat("uptime", stats.getTimeSinceStart(), sb),
            buildStat("uptime", stats.getTimeSinceStart(), sb),
            buildStat("time", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), sb),
            buildStat("version", cache.getVersion(), sb),
            buildStat("pointer_size", 0, sb), // Unsupported
            buildStat("rusage_user", 0, sb), // Unsupported
            buildStat("rusage_system", 0, sb), // Unsupported
            buildStat("curr_items", stats.getCurrentNumberOfEntries(), sb),
            buildStat("total_items", stats.getTotalNumberOfEntries(), sb),
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

   private ByteBuf buildGetResponse(String k, CacheEntry<String, byte[]> entry) {
      ByteBuf buf = buildGetHeaderBegin(k, entry, 0);
      return writeGetHeaderData(entry.getValue(), buf);
   }

   private ByteBuf buildGetHeaderBegin(String k, CacheEntry<String, byte[]> entry,
                                       int extraSpace) {
      byte[] data = entry.getValue();
      byte[] dataSize = String.valueOf(data.length).getBytes();
      byte[] key = k.getBytes();

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

   private ByteBuf writeGetHeaderData(byte[] data, ByteBuf buf) {
      buf.writeBytes(CRLFBytes);
      buf.writeBytes(data);
      buf.writeBytes(CRLFBytes);
      return buf;
   }

   private ByteBuf writeGetHeaderEnd(ByteBuf buf) {
      buf.writeBytes(END);
      return buf;
   }

   private ByteBuf buildSingleGetWithVersionResponse(String k, CacheEntry<String, byte[]> entry) {
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
      if (isTrace) log.tracef("Operation: '%s'", commandName);
      switch (commandName) {
         case "get":
            return MemcachedOperation.GetRequest;
         case "set":
            return MemcachedOperation.PutRequest;
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

//private class DelayedFlushAll(cache: AdvancedCache[String, Array[Byte]],
//                              flushFunction: AdvancedCache[String, Array[Byte]] => Unit) extends Runnable {
//   override def run() {
//      flushFunction(cache.getAdvancedCache)
//   }
//}

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

class PartialResponse {
   final Optional<ByteBuf> buffer;

   PartialResponse(Optional<ByteBuf> buffer) {
      this.buffer = buffer;
   }
}
