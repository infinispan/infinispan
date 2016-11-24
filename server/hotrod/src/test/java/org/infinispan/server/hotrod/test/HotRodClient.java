package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.NotExecutedWithPrevious;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.OperationStatus.SuccessWithPrevious;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readString;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readUnsignedInt;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readUnsignedShort;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeRangedBytes;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeString;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeUnsignedInt;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeUnsignedLong;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLEngine;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;
import org.infinispan.server.core.transport.NettyInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.hotrod.Constants;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.ProtocolFlag;
import org.infinispan.server.hotrod.Response;
import org.infinispan.server.hotrod.ServerAddress;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.KeyValuePair;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.handler.ssl.SslHandler;

/**
 * A very simple Hot Rod client for testing purposes. It's a quick and dirty client implementation. As a result, it
 * might not be very readable, particularly for readers not used to scala.
 * <p>
 * Reasons why this should not really be a trait: Storing var instances in a trait cause issues with TestNG, see:
 * http://thread.gmane.org/gmane.comp.lang.scala.user/24317
 *
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 * @since 4.1
 */
public class HotRodClient {
   final static AtomicLong idCounter = new AtomicLong();

   final String host;
   final int port;
   final String defaultCacheName;
   final int rspTimeoutSeconds;
   final byte protocolVersion;
   final SSLEngine sslEngine;
   final Channel ch;

   public HotRodClient(String host, int port, String defaultCacheName, int rspTimeoutSeconds, byte protocolVersion) {
      this(host, port, defaultCacheName, rspTimeoutSeconds, protocolVersion, null);
   }

   public HotRodClient(String host, int port, String defaultCacheName, int rspTimeoutSeconds, byte protocolVersion,
                       SSLEngine sslEngine) {
      this.host = host;
      this.port = port;
      this.defaultCacheName = defaultCacheName;
      this.rspTimeoutSeconds = rspTimeoutSeconds;
      this.protocolVersion = protocolVersion;
      this.sslEngine = sslEngine;

      ch = initializeChannel();
   }

   public String defaultCacheName() {
      return defaultCacheName;
   }

   Map<Long, Op> idToOp = new ConcurrentHashMap<>();
   SaslClient saslClient = null;
   private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
   private static final Log log = LogFactory.getLog(HotRodClient.class, Log.class);

   private Channel initializeChannel() {
      Bootstrap bootstrap = new Bootstrap();
      bootstrap.group(eventLoopGroup);
      bootstrap.handler(new NettyInitializers(new ClientChannelInitializer(this, rspTimeoutSeconds, sslEngine, protocolVersion)));
      bootstrap.channel(NioSocketChannel.class);
      bootstrap.option(ChannelOption.TCP_NODELAY, true);
      bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
      // Make a new connection.
      ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(host, port));
      // Wait until the connection is made successfully.
      Channel ch = connectFuture.syncUninterruptibly().channel();
      assertTrue(connectFuture.isSuccess());
      return ch;
   }

   public ChannelFuture stop() {
      eventLoopGroup.shutdownGracefully();
      return ch.disconnect();
   }

   public TestResponse put(byte[] k, int lifespan, int maxIdle, byte[] v) {
      return execute(0xA0, (byte) 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, (byte) 1, 0);
   }

   public TestResponse put(byte[] k, int lifespan, int maxIdle, byte[] v, byte clientIntelligence, int topologyId) {
      return execute(0xA0, (byte) 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, clientIntelligence, topologyId);
   }

   private boolean assertStatus(TestResponse resp, OperationStatus expected) {
      OperationStatus status = resp.getStatus();
      boolean isSuccess = status == expected;
      if (resp instanceof TestErrorResponse) {
         assertTrue(String.format("Status should have been '%s' but instead was: '%s', and the error message was: %s",
               expected, status, ((TestErrorResponse) resp).msg), isSuccess);
      } else {
         assertTrue(String.format(
               "Status should have been '%s' but instead was: '%s'", expected, status), isSuccess);
      }
      return isSuccess;
   }

   private byte[] k(Method m) {
      return k(m, "k-");
   }

   private byte[] k(Method m, String prefix) {
      byte[] bytes = (prefix + m.getName()).getBytes();
      log.tracef("String %s is converted to %s bytes", prefix + m.getName(), Util.printArray(bytes, true));
      return bytes;
   }

   private byte[] v(Method m) {
      return v(m, "v-");
   }

   private byte[] v(Method m, String prefix) {
      return k(m, prefix);
   }

   public void assertPut(Method m) {
      assertStatus(put(k(m), 0, 0, v(m)), Success);
   }

   public void assertPutFail(Method m) {
      Op op = new Op(0xA0, protocolVersion, (byte) 0x01, defaultCacheName, k(m), 0, 0, v(m), 0, 1, (byte) 0, 0);
      idToOp.put(op.id, op);
      ChannelFuture future = ch.writeAndFlush(op);
      future.awaitUninterruptibly();
      assertFalse(future.isSuccess());
   }

   public void assertPut(Method m, String kPrefix, String vPrefix) {
      assertStatus(put(k(m, kPrefix), 0, 0, v(m, vPrefix)), Success);
   }

   public void assertPut(Method m, int lifespan, int maxIdle) {
      assertStatus(put(k(m), lifespan, maxIdle, v(m)), Success);
   }

   public TestResponse put(String k, String v) {
      return put(k.getBytes(), 0, 0, v.getBytes());
   }

   public TestResponse put(byte[] k, int lifespan, int maxIdle, byte[] v, int flags) {
      return execute(0xA0, (byte) 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, flags);
   }

   public TestResponse putIfAbsent(byte[] k, int lifespan, int maxIdle, byte[] v) {
      return execute(0xA0, (byte) 0x05, defaultCacheName, k, lifespan, maxIdle, v, 0, (byte) 1, 0);
   }

   public TestResponse putIfAbsent(byte[] k, int lifespan, int maxIdle, byte[] v, int flags) {
      return execute(0xA0, (byte) 0x05, defaultCacheName, k, lifespan, maxIdle, v, 0, flags);
   }

   public TestResponse replace(byte[] k, int lifespan, int maxIdle, byte[] v) {
      return execute(0xA0, (byte) 0x07, defaultCacheName, k, lifespan, maxIdle, v, 0, (byte) 1, 0);
   }

   public TestResponse replace(byte[] k, int lifespan, int maxIdle, byte[] v, int flags) {
      return execute(0xA0, (byte) 0x07, defaultCacheName, k, lifespan, maxIdle, v, (byte) 0, flags);
   }

   public TestResponse replaceIfUnmodified(byte[] k, int lifespan, int maxIdle, byte[] v, long dataVersion) {
      return execute(0xA0, (byte) 0x09, defaultCacheName, k, lifespan, maxIdle, v, dataVersion, (byte) 1, 0);
   }

   public TestResponse replaceIfUnmodified(byte[] k, int lifespan, int maxIdle, byte[] v, long dataVersion, int flags) {
      return execute(0xA0, (byte) 0x09, defaultCacheName, k, lifespan, maxIdle, v, dataVersion, flags);
   }

   public TestResponse remove(byte[] k) {
      return execute(0xA0, (byte) 0x0B, defaultCacheName, k, 0, 0, null, 0, (byte) 1, 0);
   }

   public TestResponse remove(byte[] k, int flags) {
      return execute(0xA0, (byte) 0x0B, defaultCacheName, k, 0, 0, null, 0, flags);
   }

   public TestResponse removeIfUnmodified(byte[] k, int lifespan, int maxIdle, byte[] v, long dataVersion) {
      return execute(0xA0, (byte) 0x0D, defaultCacheName, k, lifespan, maxIdle, v, dataVersion, (byte) 1, 0);
   }

   public TestResponse removeIfUnmodified(byte[] k, long dataVersion, int flags) {
      return execute(0xA0, (byte) 0x0D, defaultCacheName, k, 0, 0, new byte[0], dataVersion, flags);
   }

   public TestResponse execute(int magic, byte code, String name, byte[] k, int lifespan, int maxIdle,
                               byte[] v, long dataVersion, byte clientIntelligence, int topologyId) {
      Op op = new Op(magic, protocolVersion, code, name, k, lifespan, maxIdle, v, 0, dataVersion,
            clientIntelligence, topologyId);
      return execute(op, op.id);
   }

   public TestErrorResponse executeExpectBadMagic(int magic, byte code, String name, byte[] k, int lifespan, int maxIdle,
                                                  byte[] v, long version) {
      Op op = new Op(magic, protocolVersion, code, name, k, lifespan, maxIdle, v, 0, version, (byte) 1, 0);
      return (TestErrorResponse) execute(op, 0);
   }

   public TestErrorResponse executePartial(int magic, byte code, String name, byte[] k, int lifespan, int maxIdle,
                                           byte[] v, long version) {
      Op op = new PartialOp(magic, protocolVersion, code, name, k, lifespan, maxIdle, v, 0, version, (byte) 1, 0);
      return (TestErrorResponse) execute(op, op.id);
   }

   public TestResponse execute(int magic, byte code, String name, byte[] k, int lifespan, int maxIdle,
                               byte[] v, long dataVersion, int flags) {
      Op op = new Op(magic, protocolVersion, code, name, k, lifespan, maxIdle, v, flags, dataVersion, (byte) 1, 0);
      return execute(op, op.id);
   }

   private TestResponse execute(Op op, long expectedResponseMessageId) {
      writeOp(op);
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return handler.getResponse(expectedResponseMessageId);
   }

   public boolean writeOp(Op op) {
      return writeOp(op, true);
   }

   public boolean writeOp(Op op, boolean assertSuccess) {
      idToOp.put(op.id, op);
      ChannelFuture future = ch.writeAndFlush(op);
      future.awaitUninterruptibly();
      if (assertSuccess)
         assertTrue(future.isSuccess());
      return future.isSuccess();
   }

   public TestGetResponse get(byte[] k, int flags) {
      return (TestGetResponse) get((byte) 0x03, k, flags);
   }

   public TestResponse get(String k) {
      return get((byte) 0x03, k.getBytes(), 0);
   }

   public TestGetResponse assertGet(Method m) {
      return assertGet(m, 0);
   }

   public TestGetResponse assertGet(Method m, int flags) {
      return get(k(m), flags);
   }

   public TestResponse containsKey(byte[] k, int flags) {
      return get((byte) 0x0F, k, flags);
   }

   public TestGetWithVersionResponse getWithVersion(byte[] k, int flags) {
      return (TestGetWithVersionResponse) get((byte) 0x11, k, flags);
   }

   public TestGetWithMetadataResponse getWithMetadata(byte[] k, int flags) {
      return (TestGetWithMetadataResponse) get((byte) 0x1B, k, flags);
   }

   private TestResponse get(byte code, byte[] k, int flags) {
      Op op = new Op(0xA0, protocolVersion, code, defaultCacheName, k, 0, 0, null, flags, 0, (byte) 1, 0);
      boolean writeFuture = writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      if (code == 0x03 || code == 0x11 || code == 0x0F || code == 0x1B) {
         return handler.getResponse(op.id);
      } else {
         return null;
      }
   }

   public TestResponse clear() {
      return execute(0xA0, (byte) 0x13, defaultCacheName, null, 0, 0, null, 0, (byte) 1, 0);
   }

   public Map<String, String> stats() {
      StatsOp op = new StatsOp(0xA0, protocolVersion, (byte) 0x15, defaultCacheName, (byte) 1, 0, null);
      boolean writeFuture = writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      TestStatsResponse resp = (TestStatsResponse) handler.getResponse(op.id);
      return resp.stats;
   }

   public TestResponse ping() {
      return execute(0xA0, (byte) 0x17, defaultCacheName, null, 0, 0, null, 0, (byte) 1, 0);
   }

   public TestResponse ping(byte clientIntelligence, int topologyId) {
      return execute(0xA0, (byte) 0x17, defaultCacheName, null, 0, 0, null, 0, clientIntelligence, topologyId);
   }

   public TestBulkGetResponse bulkGet() {
      return bulkGet(0);
   }

   public TestBulkGetResponse bulkGet(int count) {
      BulkGetOp op = new BulkGetOp(0xA0, protocolVersion, (byte) 0x19, defaultCacheName, (byte) 1, 0, count);
      boolean writeFuture = writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return (TestBulkGetResponse) handler.getResponse(op.id);
   }

   public TestBulkGetKeysResponse bulkGetKeys() {
      return bulkGetKeys(0);
   }

   public TestBulkGetKeysResponse bulkGetKeys(int scope) {
      BulkGetKeysOp op = new BulkGetKeysOp(0xA0, protocolVersion, (byte) 0x1D, defaultCacheName, (byte) 1, 0, scope);
      boolean writeFuture = writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return (TestBulkGetKeysResponse) handler.getResponse(op.id);
   }

   public TestQueryResponse query(byte[] query) {
      QueryOp op = new QueryOp(0xA0, protocolVersion, defaultCacheName, (byte) 1, 0, query);
      boolean writeFuture = writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return (TestQueryResponse) handler.getResponse(op.id);
   }

   public TestAuthMechListResponse authMechList() {
      AuthMechListOp op = new AuthMechListOp(0xA0, protocolVersion, (byte) 0x21, defaultCacheName, (byte) 1, 0);
      boolean writeFuture = writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return (TestAuthMechListResponse) handler.getResponse(op.id);
   }

   public TestAuthResponse auth(SaslClient sc) throws SaslException {
      saslClient = sc;
      byte[] saslResponse = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : new byte[0];
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      AuthOp op = new AuthOp(0xA0, protocolVersion, (byte) 0x23, defaultCacheName, (byte) 1, 0, saslClient.getMechanismName(), saslResponse);
      writeOp(op);
      TestAuthResponse response = (TestAuthResponse) handler.getResponse(op.id);
      while (!saslClient.isComplete() || !response.complete) {
         saslResponse = saslClient.evaluateChallenge(response.challenge);
         op = new AuthOp(0xA0, protocolVersion, (byte) 0x23, defaultCacheName, (byte) 1, 0, "", saslResponse);
         writeOp(op);
         response = (TestAuthResponse) handler.getResponse(op.id);
      }
      saslClient.dispose();
      return response;
   }

   public TestResponse addClientListener(TestClientListener listener, boolean includeState,
                                         Optional<KeyValuePair<String, List<byte[]>>> filterFactory,
                                         Optional<KeyValuePair<String, List<byte[]>>> converterFactory, boolean useRawData) {
      AddClientListenerOp op = new AddClientListenerOp(0xA0, protocolVersion, defaultCacheName,
            (byte) 1, 0, listener.getId(), includeState, filterFactory, converterFactory, useRawData);
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      handler.addClientListener(listener);
      writeOp(op);
      return handler.getResponse(op.id);
   }

   public TestResponse removeClientListener(byte[] listenerId) {
      RemoveClientListenerOp op = new RemoveClientListenerOp(0xA0, protocolVersion, defaultCacheName, (byte) 1, 0, listenerId);
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      writeOp(op);
      TestResponse response = handler.getResponse(op.id);
      if (response.getStatus() == Success) handler.removeClientListener(listenerId);
      return response;
   }

   public TestSizeResponse size() {
      SizeOp op = new SizeOp(0xA0, protocolVersion, defaultCacheName, (byte) 1, 0);
      boolean writeFuture = writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return (TestSizeResponse) handler.getResponse(op.id);
   }

   public TestGetWithMetadataResponse getStream(byte[] key, int offset) {
      GetStreamOp op = new GetStreamOp(0xA0, protocolVersion, defaultCacheName, key, 0, (byte) 1, 0, offset);
      writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return (TestGetWithMetadataResponse) handler.getResponse(op.id);
   }

   public TestResponse putStream(byte[] key, byte[] value, long version, int lifespan, int maxIdle) {
      PutStreamOp op = new PutStreamOp(0xA0, protocolVersion, defaultCacheName, key, value, lifespan, maxIdle, version, (byte)1, 0);
      writeOp(op);
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return handler.getResponse(op.id);
   }

   /*public TestPutStreamResponse putStream(byte[] k, int lifespan, int maxIdle, byte[] v, long dataVersion) {
      PutStreamOp op = new PutStreamOp(0xA0, protocolVersion, defaultCacheName, (byte) 1, 0, k, lifespan, maxIdle, v, dataVersion);
      writeOp(op);
      // Get the handler instance to retrieve the answer.
      ClientHandler handler = (ClientHandler) ch.pipeline().last();
      return (TestPutStreamResponse) handler.getResponse(op.id);
   }*/
}


class ClientChannelInitializer implements NettyInitializer {
   private final HotRodClient client;
   private final int rspTimeoutSeconds;
   private final SSLEngine sslEngine;
   private final byte protocolVersion;

   ClientChannelInitializer(HotRodClient client, int rspTimeoutSeconds, SSLEngine sslEngine, byte protocolVersion) {
      this.client = client;
      this.rspTimeoutSeconds = rspTimeoutSeconds;
      this.sslEngine = sslEngine;
      this.protocolVersion = protocolVersion;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();
      if (sslEngine != null)
         pipeline.addLast("ssl", new SslHandler(sslEngine));
      pipeline.addLast("decoder", new Decoder(client));
      pipeline.addLast("encoder", new Encoder(protocolVersion));
      pipeline.addLast("handler", new ClientHandler(rspTimeoutSeconds));
   }
}

class Encoder extends MessageToByteEncoder<Object> {
   private final byte protocolVersion;

   private static final Log log = LogFactory.getLog(Encoder.class, Log.class);

   Encoder(byte protocolVersion) {
      this.protocolVersion = protocolVersion;
   }

   @Override
   protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf buffer) throws Exception {
      log.tracef("Encode %s so that it's sent to the server", msg);
      if (msg instanceof PartialOp) {
         PartialOp partial = (PartialOp) msg;
         buffer.writeByte((byte) partial.magic); // magic
         writeUnsignedLong(partial.id, buffer); // message id
         buffer.writeByte(partial.version); // version
         buffer.writeByte(partial.code); // opcode
      } else if (msg instanceof AddClientListenerOp) {
         AddClientListenerOp op = (AddClientListenerOp) msg;
         writeHeader(op, buffer);
         writeRangedBytes(op.listenerId, buffer);
         buffer.writeByte(op.includeState ? 1 : 0);
         writeNamedFactory(op.filterFactory, buffer);
         writeNamedFactory(op.converterFactory, buffer);
         if (protocolVersion >= 21)
            buffer.writeByte(op.useRawData ? 1 : 0);
      } else if (msg instanceof RemoveClientListenerOp) {
         RemoveClientListenerOp op = (RemoveClientListenerOp) msg;
         writeHeader(op, buffer);
         writeRangedBytes(op.listenerId, buffer);
      } else if (msg instanceof Op) {
         Op op = (Op) msg;
         writeHeader(op, buffer);
         if (protocolVersion < 20)
            writeRangedBytes(new byte[0], buffer); // transaction id
         if (op.code != 0x13 && op.code != 0x15
               && op.code != 0x17 && op.code != 0x19
               && op.code != 0x1D && op.code != 0x1F
               && op.code != 0x21 && op.code != 0x23
               && op.code != 0x29) { // if it's a key based op...
            writeRangedBytes(op.key, buffer); // key length + key
            if (op.code == 0x37) {
               // GetStream has an offset
               writeUnsignedInt(((GetStreamOp)op).offset, buffer);
            }
            if (op.value != null) {
               if (op.code != 0x0D) { // If it's not removeIfUnmodified...
                  if (protocolVersion >= 22) {
                     if (op.lifespan > 0 || op.maxIdle > 0) {
                        buffer.writeByte(0); // seconds for both
                        writeUnsignedInt(op.lifespan, buffer); // lifespan
                        writeUnsignedInt(op.maxIdle, buffer); // maxIdle
                     } else {
                        buffer.writeByte(0x88);
                     }
                  } else {
                     writeUnsignedInt(op.lifespan, buffer); // lifespan
                     writeUnsignedInt(op.maxIdle, buffer); // maxIdle
                  }
               }
               if (op.code == 0x09 || op.code == 0x0D || op.code == 0x39) {
                  buffer.writeLong(op.dataVersion);
               }
               if (op.code == 0x39) {
                  // Chunk the value
                  for(int offset = 0; offset < op.value.length; ) {
                     int chunk = Math.min(op.value.length - offset, 8192);
                     writeUnsignedInt(chunk, buffer);
                     buffer.writeBytes(op.value, offset, chunk);
                     offset += chunk;
                  }
                  writeUnsignedInt(0, buffer);
               } else if (op.code != 0x0D) { // If it's not removeIfUnmodified...
                  writeRangedBytes(op.value, buffer); // value length + value
               }
            }
         } else if (op.code == 0x19) {
            writeUnsignedInt(((BulkGetOp) op).count, buffer); // Entry count
         } else if (op.code == 0x1D) {
            writeUnsignedInt(((BulkGetKeysOp) op).scope, buffer); // Bulk Get Keys Scope
         } else if (op.code == 0x1F) {
            writeRangedBytes(((QueryOp) op).query, buffer);
         } else if (op.code == 0x23) {
            AuthOp authop = (AuthOp) op;
            if (!authop.mech.isEmpty()) {
               writeRangedBytes(authop.mech.getBytes(), buffer);
            } else {
               writeUnsignedInt(0, buffer);
            }
            writeRangedBytes(((AuthOp) op).response, buffer);
         }
      }
   }

   private void writeNamedFactory(Optional<KeyValuePair<String, List<byte[]>>> namedFactory, ByteBuf buffer) {
      if (namedFactory.isPresent()) {
         KeyValuePair<String, List<byte[]>> factory = namedFactory.get();
         writeString(factory.getKey(), buffer);
         buffer.writeByte(factory.getValue().size());
         factory.getValue().forEach(bytes -> writeRangedBytes(bytes, buffer));
      } else {
         buffer.writeByte(0);
      }
   }

   private void writeHeader(Op op, ByteBuf buffer) {
      buffer.writeByte(op.magic); // magic
      writeUnsignedLong(op.id, buffer); // message id
      buffer.writeByte(op.version); // version
      buffer.writeByte(op.code); // opcode
      if (!op.cacheName.isEmpty()) {
         writeRangedBytes(op.cacheName.getBytes(), buffer); // cache name length + cache name
      } else {
         writeUnsignedInt(0, buffer); // Zero length
      }
      writeUnsignedInt(op.flags, buffer); // flags
      buffer.writeByte(op.clientIntel); // client intelligence
      writeUnsignedInt(op.topologyId, buffer); // topology id
   }
}

class Decoder extends ReplayingDecoder<Void> {
   private final HotRodClient client;

   private final static Log log = LogFactory.getLog(Decoder.class, Log.class);

   Decoder(HotRodClient client) {
      this.client = client;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
      log.trace("Decode response from server");
      buf.readUnsignedByte(); // magic byte
      long id = ExtendedByteBuf.readUnsignedLong(buf);
      HotRodOperation opCode = HotRodOperation.fromResponseOpCode((byte) buf.readUnsignedByte());
      OperationStatus status = OperationStatus.fromCode((byte) buf.readUnsignedByte());
      short topologyChangeMarker = buf.readUnsignedByte();
      Op op = client.idToOp.get(id);

      AbstractTestTopologyAwareResponse topologyChangeResponse;
      if (topologyChangeMarker == 1) {
         int topologyId = readUnsignedInt(buf);
         if (op.clientIntel == Constants.INTELLIGENCE_TOPOLOGY_AWARE) {
            int numberClusterMembers = readUnsignedInt(buf);
            ServerAddress[] viewArray = new ServerAddress[numberClusterMembers];
            for (int i = 0; i < numberClusterMembers; i++) {
               String host = readString(buf);
               int port = readUnsignedShort(buf);
               viewArray[i] = new ServerAddress(host, port);
            }
            topologyChangeResponse = new TestTopologyAwareResponse(topologyId, Arrays.asList(viewArray));
         } else if (op.clientIntel == Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE) {
            if (op.version < 20)
               topologyChangeResponse = read1xHashDistAwareHeader(buf, topologyId, op);
            else
               topologyChangeResponse = read2xHashDistAwareHeader(buf, topologyId, op);
         } else {
            throw new UnsupportedOperationException(
                  "Client intelligence " + op.clientIntel + " not supported");
         }
      } else {
         topologyChangeResponse = null;
      }

      Response resp;
      switch (opCode) {
         case STATS:
            int size = readUnsignedInt(buf);
            Map<String, String> stats = new HashMap<>();
            for (int i = 0; i < size; ++i) {
               stats.put(readString(buf), readString(buf));
            }
            resp = new TestStatsResponse(op.version, id, op.cacheName, op.clientIntel,
                  op.topologyId, topologyChangeResponse, stats);
            break;
         case PUT:
         case PUT_IF_ABSENT:
         case REPLACE:
         case REPLACE_IF_UNMODIFIED:
         case REMOVE:
         case REMOVE_IF_UNMODIFIED:
         case PUT_STREAM:
            boolean checkPrevious;
            if (op.version >= 10 && op.version <= 13) {
               checkPrevious = (op.flags & ProtocolFlag.ForceReturnPreviousValue.getValue()) == 1;
            } else {
               checkPrevious = status == SuccessWithPrevious || status == NotExecutedWithPrevious;
            }

            if (checkPrevious) {
               int length = readUnsignedInt(buf);
               if (length == 0) {
                  resp = new TestResponseWithPrevious(op.version, id, op.cacheName,
                        op.clientIntel, opCode, status, op.topologyId, topologyChangeResponse, Optional.empty());
               } else {
                  byte[] previous = new byte[length];
                  buf.readBytes(previous);
                  resp = new TestResponseWithPrevious(op.version, id, op.cacheName,
                        op.clientIntel, opCode, status, op.topologyId, topologyChangeResponse, Optional.of(previous));
               }
            } else {
               resp = new TestResponse(op.version, id, op.cacheName, op.clientIntel,
                     opCode, status, op.topologyId, topologyChangeResponse);
            }
            break;
         case CONTAINS_KEY:
         case CLEAR:
         case PING:
         case ADD_CLIENT_LISTENER:
         case REMOVE_CLIENT_LISTENER:
            resp = new TestResponse(op.version, id, op.cacheName, op.clientIntel, opCode,
                  status, op.topologyId, topologyChangeResponse);
            break;
         case GET_WITH_VERSION:
            if (status == Success) {
               long version = buf.readLong();
               Optional<byte[]> data = Optional.of(ExtendedByteBuf.readRangedBytes(buf));
               resp = new TestGetWithVersionResponse(op.version, id, op.cacheName,
                     op.clientIntel, opCode, status, op.topologyId, topologyChangeResponse, data, version);
            } else {
               resp = new TestGetWithVersionResponse(op.version, id, op.cacheName,
                     op.clientIntel, opCode, status, op.topologyId, topologyChangeResponse, Optional.empty(), 0);
            }
            break;
         case GET_WITH_METADATA:
         case GET_STREAM:
            if (status == Success) {
               long created = -1;
               int lifespan = -1;
               long lastUsed = -1;
               int maxIdle = -1;
               byte flags = buf.readByte();
               if ((flags & 0x01) != 0x01) {
                  created = buf.readLong();
                  lifespan = readUnsignedInt(buf);
               }
               if ((flags & 0x02) != 0x02) {
                  lastUsed = buf.readLong();
                  maxIdle = readUnsignedInt(buf);
               }
               long version = buf.readLong();
               Optional<byte[]> data = Optional.of(ExtendedByteBuf.readRangedBytes(buf));
               resp = new TestGetWithMetadataResponse(op.version, id, op.cacheName,
                     op.clientIntel, opCode, status, op.topologyId, topologyChangeResponse, data, version,
                     created, lifespan, lastUsed, maxIdle);
            } else {
               resp = new TestGetWithMetadataResponse(op.version, id, op.cacheName,
                     op.clientIntel, opCode, status, op.topologyId, topologyChangeResponse, Optional.empty(), 0,
                     -1, -1, -1, -1);
            }
            break;
         case GET:
            if (status == Success) {
               Optional<byte[]> data = Optional.of(ExtendedByteBuf.readRangedBytes(buf));
               resp = new TestGetResponse(op.version, id, op.cacheName, op.clientIntel,
                     opCode, status, op.topologyId, topologyChangeResponse, data);
            } else {
               resp = new TestGetResponse(op.version, id, op.cacheName, op.clientIntel,
                     opCode, status, op.topologyId, topologyChangeResponse, Optional.empty());
            }
            break;
         case BULK_GET:
            byte done = buf.readByte();
            Map<byte[], byte[]> bulkBuffer = new HashMap<>();
            while (done == 1) {
               bulkBuffer.put(ExtendedByteBuf.readRangedBytes(buf), ExtendedByteBuf.readRangedBytes(buf));
               done = buf.readByte();
            }
            resp = new TestBulkGetResponse(op.version, id, op.cacheName, op.clientIntel,
                  op.topologyId, topologyChangeResponse, bulkBuffer);
            break;
         case BULK_GET_KEYS:
            done = buf.readByte();
            Set<byte[]> bulkKeys = new HashSet<>();
            while (done == 1) {
               bulkKeys.add(ExtendedByteBuf.readRangedBytes(buf));
               done = buf.readByte();
            }
            resp = new TestBulkGetKeysResponse(op.version, id, op.cacheName, op.clientIntel,
                  op.topologyId, topologyChangeResponse, bulkKeys);
            break;
         case QUERY:
            byte[] result = ExtendedByteBuf.readRangedBytes(buf);
            resp = new TestQueryResponse(op.version, id, op.cacheName, op.clientIntel,
                  op.topologyId, topologyChangeResponse, result);
            break;
         case AUTH_MECH_LIST:
            size = readUnsignedInt(buf);
            Set<String> mechs = new HashSet<>();
            for (int i = 0; i < size; ++i) {
               mechs.add(readString(buf));
            }
            resp = new TestAuthMechListResponse(op.version, id, op.cacheName, op.clientIntel,
                  op.topologyId, topologyChangeResponse, mechs);
            break;
         case AUTH: {
            boolean complete = buf.readBoolean();
            byte[] challenge = ExtendedByteBuf.readRangedBytes(buf);
            resp = new TestAuthResponse(op.version, id, op.cacheName, op.clientIntel,
                  op.topologyId, topologyChangeResponse, complete, challenge);
            break;
         }
         case CACHE_ENTRY_CREATED_EVENT:
         case CACHE_ENTRY_MODIFIED_EVENT:
         case CACHE_ENTRY_REMOVED_EVENT:
            byte[] listenerId = ExtendedByteBuf.readRangedBytes(buf);
            byte isCustom = buf.readByte();
            boolean isRetried = buf.readByte() == 1;
            if (isCustom == 1 || isCustom == 2) {
               byte[] eventData = ExtendedByteBuf.readRangedBytes(buf);
               resp = new TestCustomEvent(client.protocolVersion, id, client.defaultCacheName, opCode, listenerId,
                     isRetried, eventData);
            } else {
               byte[] key = ExtendedByteBuf.readRangedBytes(buf);
               if (opCode == HotRodOperation.CACHE_ENTRY_REMOVED_EVENT) {
                  resp = new TestKeyEvent(client.protocolVersion, id, client.defaultCacheName, listenerId, isRetried, key);
               } else {
                  long dataVersion = buf.readLong();
                  resp = new TestKeyWithVersionEvent(client.protocolVersion, id, client.defaultCacheName,
                        opCode, listenerId, isRetried, key, dataVersion);
               }
            }
            break;
         case SIZE:
            long lsize = ExtendedByteBuf.readUnsignedLong(buf);
            resp = new TestSizeResponse(op.version, id, op.cacheName, op.clientIntel,
                  op.topologyId, topologyChangeResponse, lsize);
            break;
         case ERROR:
            if (op == null)
               resp = new TestErrorResponse((byte) 10, id, "", (short) 0, status, 0,
                     topologyChangeResponse, readString(buf));
            else
               resp = new TestErrorResponse(op.version, id, op.cacheName, op.clientIntel,
                     status, op.topologyId, topologyChangeResponse, readString(buf));
            break;
         default:
            resp = null;
            break;
      }
      if (resp != null) {
         log.tracef("Got response from server: %s", resp);
         out.add(resp);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      log.exceptionReported(cause);
   }

   private AbstractTestTopologyAwareResponse read2xHashDistAwareHeader(ByteBuf buf, int topologyId, Op op) {
      int numServersInTopo = readUnsignedInt(buf);
      List<ServerAddress> members = new ArrayList<>();
      for (int i = 0; i < numServersInTopo; ++i) {
         ServerAddress node = new ServerAddress(readString(buf), readUnsignedShort(buf));
         members.add(node);
      }

      byte hashFunction = buf.readByte();
      int numSegments = readUnsignedInt(buf);
      List<Iterable<ServerAddress>> segments = new ArrayList<>();
      if (hashFunction > 0) {
         for (int i = 1; i <= numSegments; ++i) {
            byte owners = buf.readByte();
            List<ServerAddress> membersInSegment = new ArrayList<>();
            for (int j = 1; j <= owners; ++j) {
               int index = readUnsignedInt(buf);
               membersInSegment.add(members.get(index));
            }
            segments.add(membersInSegment);
         }
      }

      return new TestHashDistAware20Response(topologyId, members, segments, hashFunction);
   }


   private AbstractTestTopologyAwareResponse read1xHashDistAwareHeader(ByteBuf buf, int topologyId, Op op) {
      int numOwners = readUnsignedShort(buf);
      byte hashFunction = buf.readByte();
      int hashSpace = readUnsignedInt(buf);
      int numServersInTopo = readUnsignedInt(buf);
      if (op.version == 10) {
         return read10HashDistAwareHeader(buf, topologyId,
               numOwners, hashFunction, hashSpace, numServersInTopo);
      } else {
         return read11HashDistAwareHeader(buf, topologyId,
               numOwners, hashFunction, hashSpace, numServersInTopo);
      }
   }


   private AbstractTestTopologyAwareResponse read10HashDistAwareHeader(ByteBuf buf, int topologyId,
                                                                       int numOwners, byte hashFunction, int hashSpace, int numServersInTopo) {
      // The exact number of topology addresses in the list is unknown
      // until we loop through the entire list and we figure out how
      // hash ids are per HotRod server (i.e. num virtual nodes > 1)
      Set<ServerAddress> members = new HashSet<>();
      Map<ServerAddress, List<Integer>> allHashIds = new HashMap<>();
      List<Integer> hashIdsOfAddr = new ArrayList<>();
      ServerAddress prevNode = null;
      for (int i = 1; i <= numServersInTopo; ++i) {
         ServerAddress node = new ServerAddress(readString(buf), readUnsignedShort(buf));
         int hashId = buf.readInt();
         if (prevNode == null || node.equals(prevNode)) {
            // First time node has been seen, so cache it
            if (prevNode == null)
               prevNode = node;

            // Add current hash id to list
            hashIdsOfAddr.add(hashId);
         } else {
            // A new node has been detected, so create the topology
            // address and store it in the view
            allHashIds.put(prevNode, hashIdsOfAddr);
            members.add(prevNode);
            prevNode = node;
            hashIdsOfAddr = new ArrayList<>();
            hashIdsOfAddr.add(hashId);
         }
         // Check for last server hash in which case just add it
         if (i == numServersInTopo) {
            allHashIds.put(prevNode, hashIdsOfAddr);
            members.add(prevNode);
         }

      }
      return new TestHashDistAware10Response(topologyId, members,
            allHashIds, numOwners, hashFunction, hashSpace);
   }


   private AbstractTestTopologyAwareResponse read11HashDistAwareHeader(ByteBuf buf, int topologyId,
                                                                       int numOwners, Byte hashFunction, int hashSpace,
                                                                       int numServersInTopo) {
      int numVirtualNodes = readUnsignedInt(buf);
      Map<ServerAddress, Integer> hashToAddress = new HashMap<>();
      for (int i = 1; i <= numServersInTopo; ++i) {
         hashToAddress.put(new ServerAddress(readString(buf), readUnsignedShort(buf)), buf.readInt());
      }

      return new TestHashDistAware11Response(topologyId, hashToAddress,
            numOwners, hashFunction, hashSpace, numVirtualNodes);
   }
}

class ClientHandler extends ChannelInboundHandlerAdapter {
   private static final Log log = LogFactory.getLog(ClientHandler.class, Log.class);
   final int rspTimeoutSeconds;

   ClientHandler(int rspTimeoutSeconds) {
      this.rspTimeoutSeconds = rspTimeoutSeconds;
   }

   private Map<Long, TestResponse> responses = new ConcurrentHashMap<>();
   private Map<WrappedByteArray, TestClientListener> clientListeners = new ConcurrentHashMap<>();

   void addClientListener(TestClientListener listener) {
      clientListeners.put(new WrappedByteArray(listener.getId()), listener);
   }

   void removeClientListener(byte[] listenerId) {
      clientListeners.remove(new WrappedByteArray(listenerId));
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof TestKeyWithVersionEvent) {
         TestKeyWithVersionEvent e = (TestKeyWithVersionEvent) msg;
         switch (e.getOperation()) {
            case CACHE_ENTRY_CREATED_EVENT:
               clientListeners.get(new WrappedByteArray(e.listenerId)).onCreated(e);
               break;
            case CACHE_ENTRY_MODIFIED_EVENT:
               clientListeners.get(new WrappedByteArray(e.listenerId)).onModified(e);
               break;
         }
      } else if (msg instanceof TestKeyEvent) {
         TestKeyEvent e = (TestKeyEvent) msg;
         clientListeners.get(new WrappedByteArray(e.listenerId)).onRemoved(e);
      } else if (msg instanceof TestCustomEvent) {
         TestCustomEvent e = (TestCustomEvent) msg;
         clientListeners.get(new WrappedByteArray(e.listenerId)).onCustom(e);
      } else if (msg instanceof TestResponse) {
         TestResponse resp = (TestResponse) msg;
         log.tracef("Put %s in responses", resp);
         responses.put(resp.getMessageId(), resp);
      } else {
         throw new IllegalArgumentException("Unsupport object: " + msg);
      }
   }

   TestResponse getResponse(long messageId) {
      // Very TODO very primitive way of waiting for a response. Convert to a Future
      int i = 0;
      TestResponse v = null;
      do {
         v = responses.get(messageId);
         if (v == null) {
            TestingUtil.sleepThread(100);
            i += 1;
         }
      }
      while (v == null && i < (rspTimeoutSeconds * 10));

      return v;
   }
}

class PartialOp extends Op {

   public PartialOp(int magic, byte version, byte code, String cacheName, byte[] key, int lifespan, int maxIdle,
                    byte[] value, int flags, long dataVersion, byte clientIntel, int topologyId) {
      super(magic, version, code, cacheName, key, lifespan, maxIdle, value, flags, dataVersion, clientIntel, topologyId);
   }
}

abstract class AbstractOp extends Op {
   public AbstractOp(int magic, byte version, byte code, String cacheName, byte clientIntel, int topologyId) {
      super(magic, version, code, cacheName, null, 0, 0, null, 0, 0, clientIntel, topologyId);
   }
}

class StatsOp extends AbstractOp {
   final String statName;

   public StatsOp(int magic, byte version, byte code, String cacheName, byte clientIntel, int topologyId, String statName) {
      super(magic, version, code, cacheName, clientIntel, topologyId);
      this.statName = statName;
   }
}

class BulkGetOp extends AbstractOp {
   final int count;

   public BulkGetOp(int magic, byte version, byte code, String cacheName, byte clientIntel, int topologyId, int count) {
      super(magic, version, code, cacheName, clientIntel, topologyId);
      this.count = count;
   }
}

class BulkGetKeysOp extends AbstractOp {
   final int scope;

   public BulkGetKeysOp(int magic, byte version, byte code, String cacheName, byte clientIntel, int topologyId, int scope) {
      super(magic, version, code, cacheName, clientIntel, topologyId);
      this.scope = scope;
   }
}


class QueryOp extends AbstractOp {
   final byte[] query;

   public QueryOp(int magic, byte version, String cacheName, byte clientIntel, int topologyId, byte[] query) {
      super(magic, version, (byte) 0x1F, cacheName, clientIntel, topologyId);
      this.query = query;
   }
}

class AddClientListenerOp extends AbstractOp {
   final byte[] listenerId;
   final boolean includeState;
   final Optional<KeyValuePair<String, List<byte[]>>> filterFactory;
   final Optional<KeyValuePair<String, List<byte[]>>> converterFactory;
   final boolean useRawData;

   public AddClientListenerOp(int magic, byte version, String cacheName, byte clientIntel, int topologyId,
                              byte[] listenerId, boolean includeState, Optional<KeyValuePair<String, List<byte[]>>> filterFactory,
                              Optional<KeyValuePair<String, List<byte[]>>> converterFactory, boolean useRawData) {
      super(magic, version, (byte) 0x25, cacheName, clientIntel, topologyId);
      this.listenerId = listenerId;
      this.includeState = includeState;
      this.filterFactory = filterFactory;
      this.converterFactory = converterFactory;
      this.useRawData = useRawData;
   }
}

class RemoveClientListenerOp extends AbstractOp {
   final byte[] listenerId;

   public RemoveClientListenerOp(int magic, byte version, String cacheName, byte clientIntel, int topologyId,
                                 byte[] listenerId) {
      super(magic, version, (byte) 0x27, cacheName, clientIntel, topologyId);
      this.listenerId = listenerId;
   }
}

class AuthMechListOp extends AbstractOp {
   public AuthMechListOp(int magic, byte version, byte code, String cacheName, byte clientIntel, int topologyId) {
      super(magic, version, code, cacheName, clientIntel, topologyId);
   }
}

class AuthOp extends AbstractOp {
   final String mech;
   final byte[] response;

   public AuthOp(int magic, byte version, byte code, String cacheName, byte clientIntel, int topologyId, String mech,
                 byte[] response) {
      super(magic, version, code, cacheName, clientIntel, topologyId);
      this.mech = mech;
      this.response = response;
   }
}

class SizeOp extends AbstractOp {
   public SizeOp(int magic, byte version, String cacheName, byte clientIntel, int topologyId) {
      super(magic, version, (byte) 0x29, cacheName, clientIntel, topologyId);
   }
}

class GetStreamOp extends Op {
   final int offset;

   public GetStreamOp(int magic, byte version, String cacheName, byte[] key, int flags, byte clientIntel, int topologyId, int offset) {
      super(magic, version, (byte)0x37, cacheName, key, -1, -1, null, flags, 0, clientIntel, topologyId);
      this.offset = offset;
   }
}

class PutStreamOp extends Op {
      public PutStreamOp(int magic, byte version, String cacheName, byte[] key, byte[] value, int lifespan, int maxIdle, long dataVersion, byte clientIntel, int topologyId) {
      super(magic, version, (byte)0x39, cacheName, key, lifespan, maxIdle, value, 0, dataVersion, clientIntel, topologyId);
   }
}

class ServerNode {
   final String host;
   final int port;

   ServerNode(String host, int port) {
      this.host = host;
      this.port = port;
   }
}

class TestTopologyAwareResponse extends AbstractTestTopologyAwareResponse {
   protected TestTopologyAwareResponse(int topologyId, Collection<ServerAddress> members) {
      super(topologyId, members);
   }
}

class TestHashDistAware10Response extends AbstractTestTopologyAwareResponse {

   final Map<ServerAddress, List<Integer>> hashIds;
   final int numOwners;
   final byte hashFunction;
   final int hashSpace;

   protected TestHashDistAware10Response(int topologyId, Collection<ServerAddress> members, Map<ServerAddress,
         List<Integer>> hashIds, int numOwners, byte hashFunction, int hashSpace) {
      super(topologyId, members);
      this.hashIds = hashIds;
      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.hashSpace = hashSpace;
   }
}

class TestHashDistAware11Response extends AbstractTestTopologyAwareResponse {

   final Map<ServerAddress, Integer> membersToHash;
   final int numOwners;
   final byte hashFunction;
   final int hashSpace;
   final int numVirtualNodes;

   protected TestHashDistAware11Response(int topologyId, Map<ServerAddress, Integer> membersToHash, int numOwners,
                                         byte hashFunction, int hashSpace, int numVirtualNodes) {
      super(topologyId, membersToHash.keySet());
      this.membersToHash = membersToHash;
      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.hashSpace = hashSpace;
      this.numVirtualNodes = numVirtualNodes;
   }
}

class TestHashDistAware20Response extends AbstractTestTopologyAwareResponse {
   final List<Iterable<ServerAddress>> segments;
   final byte hashFunction;

   protected TestHashDistAware20Response(int topologyId, Collection<ServerAddress> members,
                                         List<Iterable<ServerAddress>> segments, byte hashFunction) {
      super(topologyId, members);
      this.segments = segments;
      this.hashFunction = hashFunction;
   }
}
