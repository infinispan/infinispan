package org.infinispan.server.hotrod.test

import java.net.InetSocketAddress
import org.testng.Assert._
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.OperationResponse._
import collection.mutable
import collection.immutable
import java.lang.reflect.Method
import HotRodTestingUtil._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import mutable.ListBuffer
import org.infinispan.test.TestingUtil
import org.infinispan.commons.util.Util
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.hotrod._
import java.lang.IllegalStateException
import java.lang.StringBuilder
import javax.net.ssl.SSLEngine
import io.netty.handler.ssl.SslHandler
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.bootstrap.Bootstrap
import io.netty.handler.codec.{ReplayingDecoder, MessageToByteEncoder}
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.nio.NioSocketChannel
import scala.Some
import javax.security.sasl.SaslClient
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8
import org.infinispan.commons.equivalence.{ByteArrayEquivalence, AnyEquivalence}

/**
 * A very simple Hot Rod client for testing purposes. It's a quick and dirty client implementation.
 * As a result, it might not be very readable, particularly for readers not used to scala.
 *
 * Reasons why this should not really be a trait:
 * Storing var instances in a trait cause issues with TestNG, see:
 *   http://thread.gmane.org/gmane.comp.lang.scala.user/24317
 *
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 * @since 4.1
 */
class HotRodClient(host: String, port: Int, val defaultCacheName: String, rspTimeoutSeconds: Int, val protocolVersion: Byte, sslEngine: SSLEngine = null) extends Log {
   val idToOp = new ConcurrentHashMap[Long, Op]
   var saslClient: SaslClient = null

   private lazy val ch: Channel = {
      val eventLoopGroup = new NioEventLoopGroup()
      val bootstrap: Bootstrap = new Bootstrap()
      bootstrap.group(eventLoopGroup)
      bootstrap.handler(new ClientChannelInitializer(this, rspTimeoutSeconds, sslEngine, protocolVersion))
      bootstrap.channel(classOf[NioSocketChannel])
      bootstrap.option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
      bootstrap.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      // Make a new connection.
      val connectFuture = bootstrap.connect(new InetSocketAddress(host, port))
      // Wait until the connection is made successfully.
      val ch = connectFuture.syncUninterruptibly().channel
      assertTrue(connectFuture.isSuccess)
      ch
   }

   def stop = ch.disconnect

   def put(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): TestResponse =
      execute(0xA0, 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, 1 ,0)

   def put(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], clientIntelligence: Byte, topologyId: Int): TestResponse =
      execute(0xA0, 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, clientIntelligence, topologyId)

   def assertPut(m: Method) {
      assertStatus(put(k(m) , 0, 0, v(m)), Success)
   }

   def assertPutFail(m: Method) {
      val op = new Op(0xA0, protocolVersion, 0x01, defaultCacheName, k(m), 0, 0, v(m), 0, 1 , 0, 0)
      idToOp.put(op.id, op)
      val future = ch.writeAndFlush(op)
      future.awaitUninterruptibly
      assertFalse(future.isSuccess)
   }

   def assertPut(m: Method, kPrefix: String, vPrefix: String) {
      assertStatus(put(k(m, kPrefix) , 0, 0, v(m, vPrefix)), Success)
   }

   def assertPut(m: Method, lifespan: Int, maxIdle: Int) {
      assertStatus(put(k(m) , lifespan, maxIdle, v(m)), Success)
   }

   def put(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): TestResponse =
      execute(0xA0, 0x01, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)

   def putIfAbsent(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): TestResponse =
      execute(0xA0, 0x05, defaultCacheName, k, lifespan, maxIdle, v, 0, 1 ,0)

   def putIfAbsent(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): TestResponse =
      execute(0xA0, 0x05, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)

   def replace(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte]): TestResponse =
      execute(0xA0, 0x07, defaultCacheName, k, lifespan, maxIdle, v, 0, 1 ,0)

   def replace(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte], flags: Int): TestResponse =
      execute(0xA0, 0x07, defaultCacheName, k, lifespan, maxIdle, v, 0, flags)

   def replaceIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte],
                           dataVersion: Long): TestResponse =
      execute(0xA0, 0x09, defaultCacheName, k, lifespan, maxIdle, v, dataVersion, 1 ,0)

   def replaceIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte],
                           dataVersion: Long, flags: Int): TestResponse =
      execute(0xA0, 0x09, defaultCacheName, k, lifespan, maxIdle, v, dataVersion, flags)

   def remove(k: Array[Byte]): TestResponse =
      execute(0xA0, 0x0B, defaultCacheName, k, 0, 0, null, 0, 1 ,0)

   def remove(k: Array[Byte], flags: Int): TestResponse =
      execute(0xA0, 0x0B, defaultCacheName, k, 0, 0, null, 0, flags)

   def removeIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte],
                          dataVersion: Long): TestResponse =
      execute(0xA0, 0x0D, defaultCacheName, k, lifespan, maxIdle, v, dataVersion, 1 ,0)

   def removeIfUnmodified(k: Array[Byte], lifespan: Int, maxIdle: Int, v: Array[Byte],
                          dataVersion: Long, flags: Int): TestResponse =
      execute(0xA0, 0x0D, defaultCacheName, k, lifespan, maxIdle, v, dataVersion, flags)

   def execute(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
               v: Array[Byte], dataVersion: Long, clientIntelligence: Byte, topologyId: Int): TestResponse = {
      val op = new Op(magic, protocolVersion, code, name, k, lifespan, maxIdle, v, 0, dataVersion,
                      clientIntelligence, topologyId)
      execute(op, op.id)
   }

   def executeExpectBadMagic(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
                           v: Array[Byte], version: Long): TestErrorResponse = {
      val op = new Op(magic, protocolVersion, code, name, k, lifespan, maxIdle, v, 0, version, 1, 0)
      execute(op, 0).asInstanceOf[TestErrorResponse]
   }

   def executePartial(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
                      v: Array[Byte], version: Long): TestErrorResponse = {
      val op = new PartialOp(magic, protocolVersion, code, name, k, lifespan, maxIdle, v, 0, version, 1, 0)
      execute(op, op.id).asInstanceOf[TestErrorResponse]
   }

   def execute(magic: Int, code: Byte, name: String, k: Array[Byte], lifespan: Int, maxIdle: Int,
               v: Array[Byte], dataVersion: Long, flags: Int): TestResponse = {
      val op = new Op(magic, protocolVersion, code, name, k, lifespan, maxIdle, v, flags, dataVersion, 1, 0)
      execute(op, op.id)
   }

   private def execute(op: Op, expectedResponseMessageId: Long): TestResponse = {
      writeOp(op)
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      handler.getResponse(expectedResponseMessageId)
   }

   private def writeOp(op: Op) {
      idToOp.put(op.id, op)
      val future = ch.writeAndFlush(op)
      future.awaitUninterruptibly
      assertTrue(future.isSuccess)
   }

   def get(k: Array[Byte], flags: Int): TestGetResponse = {
      get(0x03, k, flags).asInstanceOf[TestGetResponse]
   }

   def assertGet(m: Method): TestGetResponse = assertGet(m, 0)

   def assertGet(m: Method, flags: Int): TestGetResponse = get(k(m), flags)

   def containsKey(k: Array[Byte], flags: Int): TestResponse = {
      get(0x0F, k, flags)
   }

   def getWithVersion(k: Array[Byte], flags: Int): TestGetWithVersionResponse =
      get(0x11, k, flags).asInstanceOf[TestGetWithVersionResponse]

   def getWithMetadata(k: Array[Byte], flags: Int): TestGetWithMetadataResponse =
      get(0x1B, k, flags).asInstanceOf[TestGetWithMetadataResponse]

   private def get(code: Byte, k: Array[Byte], flags: Int): TestResponse = {
      val op = new Op(0xA0, protocolVersion, code, defaultCacheName, k, 0, 0, null, flags, 0, 1, 0)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      if (code == 0x03 || code == 0x11 || code == 0x0F || code == 0x1B) {
         handler.getResponse(op.id)
      } else {
         null
      }
   }

   def clear: TestResponse = execute(0xA0, 0x13, defaultCacheName, null, 0, 0, null, 0, 1 ,0)

   def stats: Map[String, String] = {
      val op = new StatsOp(0xA0, protocolVersion, 0x15, defaultCacheName, 1, 0, null)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      val resp = handler.getResponse(op.id).asInstanceOf[TestStatsResponse]
      resp.stats
   }

   def ping: TestResponse = execute(0xA0, 0x17, defaultCacheName, null, 0, 0, null, 0, 1 ,0)

   def ping(clientIntelligence: Byte, topologyId: Int): TestResponse =
      execute(0xA0, 0x17, defaultCacheName, null, 0, 0, null, 0, clientIntelligence, topologyId)

   def bulkGet: TestBulkGetResponse = bulkGet(0)

   def bulkGet(count: Int): TestBulkGetResponse = {
      val op = new BulkGetOp(0xA0, protocolVersion, 0x19, defaultCacheName, 1, 0, count)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestBulkGetResponse]
   }

   def bulkGetKeys: TestBulkGetKeysResponse = {
   	val op = new BulkGetKeysOp(0xA0, protocolVersion, 0x1D, defaultCacheName, 1, 0, 0)
      val writeFuture = writeOp(op)
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestBulkGetKeysResponse]
   }

   def bulkGetKeys(scope: Int): TestBulkGetKeysResponse = {
   	val op = new BulkGetKeysOp(0xA0, protocolVersion, 0x1D, defaultCacheName, 1, 0, scope)
      val writeFuture = writeOp(op)
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestBulkGetKeysResponse]
   }

   def query(query: Array[Byte]): TestQueryResponse = {
      val op = new QueryOp(0xA0, protocolVersion, defaultCacheName, 1, 0, query)
      val writeFuture = writeOp(op)
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestQueryResponse]
   }

   def authMechList(): TestAuthMechListResponse = {
      val op = new AuthMechListOp(0xA0, protocolVersion, 0x21, defaultCacheName, 1, 0)
      val writeFuture = writeOp(op)
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestAuthMechListResponse]
   }

   def auth(sc: SaslClient): TestAuthResponse = {
      saslClient = sc
      var saslResponse = if (saslClient.hasInitialResponse()) saslClient.evaluateChallenge(new Array[Byte](0)) else new Array[Byte](0)
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      var op = new AuthOp(0xA0, protocolVersion, 0x23, defaultCacheName, 1, 0, saslClient.getMechanismName(), saslResponse)
      writeOp(op)
      var response = handler.getResponse(op.id).asInstanceOf[TestAuthResponse]
      while (!saslClient.isComplete() || !response.complete) {
         saslResponse = saslClient.evaluateChallenge(response.challenge)
         op = new AuthOp(0xA0, protocolVersion, 0x23, defaultCacheName, 1, 0, "", saslResponse)
         writeOp(op)
         response = handler.getResponse(op.id).asInstanceOf[TestAuthResponse]
      }
      saslClient.dispose
      response
   }

   def addClientListener(listener: TestClientListener,
           filterFactory: NamedFactory, converterFactory: NamedFactory): TestResponse = {
      val op = new AddClientListenerOp(0xA0, protocolVersion, defaultCacheName,
         1, 0, listener.getId, filterFactory, converterFactory)
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      handler.addClientListener(listener)
      writeOp(op)
      handler.getResponse(op.id)
   }

   def removeClientListener(listenerId: Bytes): TestResponse = {
      val op = new RemoveClientListenerOp(0xA0, protocolVersion, defaultCacheName, 1, 0, listenerId)
      val handler = ch.pipeline.last.asInstanceOf[ClientHandler]
      writeOp(op)
      val response = handler.getResponse(op.id)
      if (response.status == Success) handler.removeClientListener(listenerId)
      response
   }

}

private class ClientChannelInitializer(client: HotRodClient, rspTimeoutSeconds: Int, sslEngine: SSLEngine, protocolVersion: Byte) extends ChannelInitializer[Channel] {

   override def initChannel(ch: Channel) = {
      val pipeline = ch.pipeline
      if (sslEngine != null)
         pipeline.addLast("ssl", new SslHandler(sslEngine))
      pipeline.addLast("decoder", new Decoder(client))
      pipeline.addLast("encoder", new Encoder(protocolVersion))
      pipeline.addLast("handler", new ClientHandler(rspTimeoutSeconds))
   }
}

private class Encoder(protocolVersion: Byte) extends MessageToByteEncoder[Object] {

   override def encode(ctx: ChannelHandlerContext, msg: AnyRef, buffer: ByteBuf): Unit = {
      trace("Encode %s so that it's sent to the server", msg)
      msg match {
         case partial: PartialOp => {
            buffer.writeByte(partial.magic.asInstanceOf[Byte]) // magic
            writeUnsignedLong(partial.id, buffer) // message id
            buffer.writeByte(partial.version) // version
            buffer.writeByte(partial.code) // opcode
         }
         case op: AddClientListenerOp =>
            writeHeader(op, buffer)
            writeRangedBytes(op.listenerId, buffer)
            writeNamedFactory(op.filterFactory, buffer)
            writeNamedFactory(op.converterFactory, buffer)
         case op: RemoveClientListenerOp =>
            writeHeader(op, buffer)
            writeRangedBytes(op.listenerId, buffer)
         case op: Op => {
            writeHeader(op, buffer)
            if (protocolVersion < 20)
               writeRangedBytes(new Array[Byte](0), buffer) // transaction id
            if (op.code != 0x13 && op.code != 0x15
                    && op.code != 0x17 && op.code != 0x19
                    && op.code != 0x1D && op.code != 0x1F
                    && op.code != 0x21 && op.code != 0x23) { // if it's a key based op...
               writeRangedBytes(op.key, buffer) // key length + key
               if (op.value != null) {
                  if (op.code != 0x0D) { // If it's not removeIfUnmodified...
                     writeUnsignedInt(op.lifespan, buffer) // lifespan
                     writeUnsignedInt(op.maxIdle, buffer) // maxIdle
                  }
                  if (op.code == 0x09 || op.code == 0x0D) {
                     buffer.writeLong(op.dataVersion)
                  }
                  if (op.code != 0x0D) { // If it's not removeIfUnmodified...
                     writeRangedBytes(op.value, buffer) // value length + value
                  }
               }
            } else if (op.code == 0x19) {
               writeUnsignedInt(op.asInstanceOf[BulkGetOp].count, buffer) // Entry count
            } else if (op.code == 0x1D) {
               writeUnsignedInt(op.asInstanceOf[BulkGetKeysOp].scope, buffer) // Bulk Get Keys Scope
            } else if (op.code == 0x1F) {
               writeRangedBytes(op.asInstanceOf[QueryOp].query, buffer)
            } else if (op.code == 0x23) {
               val authop = op.asInstanceOf[AuthOp]
               if (!authop.mech.isEmpty) {
                  writeRangedBytes(authop.mech.getBytes(), buffer)
               } else {
                  writeUnsignedInt(0, buffer)
               }
               writeRangedBytes(op.asInstanceOf[AuthOp].response, buffer)
            }
         }
      }
   }

   private def writeNamedFactory(namedFactory: NamedFactory, buffer: ByteBuf): Unit = {
      namedFactory match {
         case Some(factory) =>
            writeString(factory._1, buffer)
            buffer.writeByte(factory._2.length)
            factory._2.foreach(writeRangedBytes(_, buffer))
         case None => buffer.writeByte(0)
      }
   }

   private def writeHeader(op: Op,  buffer: ByteBuf): Unit = {
      buffer.writeByte(op.magic.asInstanceOf[Byte]) // magic
      writeUnsignedLong(op.id, buffer) // message id
      buffer.writeByte(op.version) // version
      buffer.writeByte(op.code) // opcode
      if (!op.cacheName.isEmpty) {
         writeRangedBytes(op.cacheName.getBytes, buffer) // cache name length + cache name
      } else {
         writeUnsignedInt(0, buffer) // Zero length
      }
      writeUnsignedInt(op.flags, buffer) // flags
      buffer.writeByte(op.clientIntel) // client intelligence
      writeUnsignedInt(op.topologyId, buffer) // topology id
   }

}

object HotRodClient {
   val idCounter = new AtomicLong
}

private class Decoder(client: HotRodClient) extends ReplayingDecoder[Void] with Log with Constants {

   override def decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: java.util.List[AnyRef]): Unit = {
      trace("Decode response from server")
      buf.readUnsignedByte // magic byte
      val id = readUnsignedLong(buf)
      val opCode = OperationResponse.apply(buf.readUnsignedByte)
      val status = OperationStatus.apply(buf.readUnsignedByte)
      val topologyChangeMarker = buf.readUnsignedByte
      val op = client.idToOp.get(id)
      val topologyChangeResponse =
         if (topologyChangeMarker == 1) {
            val topologyId = readUnsignedInt(buf)
            if (op.clientIntel == INTELLIGENCE_TOPOLOGY_AWARE) {
               val numberClusterMembers = readUnsignedInt(buf)
               val viewArray = new Array[ServerAddress](numberClusterMembers)
               for (i <- 0 until numberClusterMembers) {
                  val host = readString(buf)
                  val port = readUnsignedShort(buf)
                  viewArray(i) = new ServerAddress(host, port)
               }
               Some(TestTopologyAwareResponse(topologyId, viewArray.toList))
            } else if (op.clientIntel == INTELLIGENCE_HASH_DISTRIBUTION_AWARE) {
               if (op.version < 20)
                  read1xHashDistAwareHeader(buf, topologyId, op)
               else
                  read2xHashDistAwareHeader(buf, topologyId, op)
            } else {
               throw new UnsupportedOperationException(
                  "Client intelligence " + op.clientIntel + " not supported");
            }
         } else {
            None
         }

      val resp: Response = opCode match {
         case StatsResponse => {
            val size = readUnsignedInt(buf)
            val stats = mutable.Map.empty[String, String]
            for (i <- 1 to size) {
               stats += (readString(buf) -> readString(buf))
            }
            new TestStatsResponse(op.version, id, op.cacheName, op.clientIntel,
                  immutable.Map[String, String]() ++ stats, op.topologyId, topologyChangeResponse)
         }
         case PutResponse | PutIfAbsentResponse | ReplaceResponse | ReplaceIfUnmodifiedResponse
              | RemoveResponse | RemoveIfUnmodifiedResponse => {
            if ((op.flags & ProtocolFlag.ForceReturnPreviousValue.id) == 1) {
               val length = readUnsignedInt(buf)
               if (length == 0) {
                  new TestResponseWithPrevious(op.version, id, op.cacheName,
                        op.clientIntel, opCode, status, op.topologyId, None,
                        topologyChangeResponse)
               } else {
                  val previous = new Array[Byte](length)
                  buf.readBytes(previous)
                  new TestResponseWithPrevious(op.version, id, op.cacheName,
                     op.clientIntel, opCode, status, op.topologyId, Some(previous),
                     topologyChangeResponse)
               }
            } else new TestResponse(op.version, id, op.cacheName, op.clientIntel,
                     opCode, status, op.topologyId, topologyChangeResponse)
         }
         case ContainsKeyResponse | ClearResponse | PingResponse | AddClientListenerResponse | RemoveClientListenerResponse =>
            new TestResponse(op.version, id, op.cacheName, op.clientIntel, opCode,
                  status, op.topologyId, topologyChangeResponse)
         case GetWithVersionResponse  => {
            if (status == Success) {
               val version = buf.readLong
               val data = Some(readRangedBytes(buf))
               new TestGetWithVersionResponse(op.version, id, op.cacheName,
                  op.clientIntel, opCode, status, op.topologyId, data, version,
                  topologyChangeResponse)
            } else{
               new TestGetWithVersionResponse(op.version, id, op.cacheName,
                     op.clientIntel, opCode, status, op.topologyId, None, 0,
                     topologyChangeResponse)
            }
         }
         case GetWithMetadataResponse => {
            if (status == Success) {
               var created = -1l
               var lifespan = -1
               var lastUsed = -1l
               var maxIdle = -1
               val flags = buf.readByte()
               if ((flags & 0x01) != 0x01) {
                  created = buf.readLong
                  lifespan = readUnsignedInt(buf)
               }
               if ((flags & 0x02) != 0x02) {
                  lastUsed = buf.readLong
                  maxIdle = readUnsignedInt(buf)
               }
               val version = buf.readLong
               val data = Some(readRangedBytes(buf))
               new TestGetWithMetadataResponse(op.version, id, op.cacheName,
                     op.clientIntel, opCode, status, op.topologyId, data, version,
                     created, lifespan, lastUsed, maxIdle, topologyChangeResponse)
            } else{
               new TestGetWithMetadataResponse(op.version, id, op.cacheName,
                     op.clientIntel, opCode, status, op.topologyId, None, 0,
                     -1, -1, -1, -1, topologyChangeResponse)
            }
         }
         case GetResponse => {
            if (status == Success) {
               val data = Some(readRangedBytes(buf))
               new TestGetResponse(op.version, id, op.cacheName, op.clientIntel,
                     opCode, status, op.topologyId, data, topologyChangeResponse)
            } else{
               new TestGetResponse(op.version, id, op.cacheName, op.clientIntel,
                     opCode, status, op.topologyId, None, topologyChangeResponse)
            }
         }
         case BulkGetResponse => {
            var done = buf.readByte
            val bulkBuffer = mutable.Map.empty[Array[Byte], Array[Byte]]
            while (done == 1) {
               bulkBuffer += (readRangedBytes(buf) -> readRangedBytes(buf))
               done = buf.readByte
            }
            val bulk = immutable.Map[Array[Byte], Array[Byte]]() ++ bulkBuffer
            new TestBulkGetResponse(op.version, id, op.cacheName, op.clientIntel,
                  bulk, op.topologyId, topologyChangeResponse)
         }
         case BulkGetKeysResponse => {
            var done = buf.readByte
            val bulkBuffer = mutable.Set.empty[Array[Byte]]
            while (done == 1) {
               bulkBuffer += readRangedBytes(buf)
               done = buf.readByte
            }
            val bulk = immutable.Set[Array[Byte]]() ++ bulkBuffer
            new TestBulkGetKeysResponse(op.version, id, op.cacheName, op.clientIntel,
                  bulk, op.topologyId, topologyChangeResponse)
         }
         case QueryResponse => {
            val result = readRangedBytes(buf)
            new TestQueryResponse(op.version, id, op.cacheName, op.clientIntel,
               result, op.topologyId, topologyChangeResponse)
         }
         case AuthMechListResponse => {
            val size = readUnsignedInt(buf)
            val mechs = mutable.Set.empty[String]
            for (i <- 1 to size) {
               mechs += readString(buf)
            }
            new TestAuthMechListResponse(op.version, id, op.cacheName, op.clientIntel,
               mechs.toSet, op.topologyId, topologyChangeResponse)
         }
         case AuthResponse => {
            val complete = buf.readBoolean
            val challenge = readRangedBytes(buf)
            new TestAuthResponse(op.version, id, op.cacheName, op.clientIntel,
                     complete, challenge, op.topologyId, topologyChangeResponse)
         }
         case CacheEntryCreatedEventResponse | CacheEntryModifiedEventResponse | CacheEntryRemovedEventResponse =>
            val listenerId = readRangedBytes(buf)
            val isCustom = buf.readByte()
            if (isCustom == 1) {
               val eventData = readRangedBytes(buf)
               new TestCustomEvent(client.protocolVersion, id, client.defaultCacheName, opCode, listenerId, eventData)
            } else {
               val key = readRangedBytes(buf)
               if (opCode == CacheEntryRemovedEventResponse) {
                  new TestKeyEvent(client.protocolVersion, id, client.defaultCacheName, listenerId, key)
               } else {
                  val dataVersion = buf.readLong()
                  new TestKeyWithVersionEvent(client.protocolVersion, id, client.defaultCacheName,
                     opCode, listenerId, key, dataVersion)
               }
            }
         case ErrorResponse => {
            if (op == null)
               new TestErrorResponse(10, id, "", 0, status, 0,
                     readString(buf), topologyChangeResponse)
            else
               new TestErrorResponse(op.version, id, op.cacheName, op.clientIntel,
                     status, op.topologyId, readString(buf), topologyChangeResponse)
         }

      }
      if (resp != null) {
         trace("Got response from server: %s", resp)
         out.add(resp)
      }
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
      logExceptionReported(cause)
   }

   private def read2xHashDistAwareHeader(buf: ByteBuf, topologyId: Int, op: Op): Option[AbstractTestTopologyAwareResponse] = {
      val numServersInTopo = readUnsignedInt(buf)
      var members = mutable.ListBuffer[ServerAddress]()
      for (i <- 1 to numServersInTopo) {
         val node = new ServerAddress(readString(buf), readUnsignedShort(buf))
         members += node
      }

      val hashFunction = buf.readByte
      val numSegments = readUnsignedInt(buf)
      var segments = mutable.ListBuffer[Iterable[ServerAddress]]()
      for (i <- 1 to numSegments) {
         val owners = buf.readByte()
         var membersInSegment = mutable.ListBuffer[ServerAddress]()
         for (j <- 1 to owners) {
            val index = readUnsignedInt(buf)
            membersInSegment += members(index)
         }
         segments += membersInSegment.toList
      }

      Some(TestHashDistAware20Response(topologyId, members.toList, segments.toList, hashFunction))
   }

   private def read1xHashDistAwareHeader(buf: ByteBuf, topologyId: Int, op: Op): Option[AbstractTestTopologyAwareResponse] = {
      val numOwners = readUnsignedShort(buf)
      val hashFunction = buf.readByte
      val hashSpace = readUnsignedInt(buf)
      val numServersInTopo = readUnsignedInt(buf)
      op.version match {
         case 10 => read10HashDistAwareHeader(buf, topologyId,
            numOwners, hashFunction, hashSpace, numServersInTopo)
         case _ => read11HashDistAwareHeader(buf, topologyId,
            numOwners, hashFunction, hashSpace, numServersInTopo)
      }
   }

   private def read10HashDistAwareHeader(buf: ByteBuf, topologyId: Int,
            numOwners: Int, hashFunction: Byte, hashSpace: Int,
            numServersInTopo: Int): Option[AbstractTestTopologyAwareResponse] = {
      // The exact number of topology addresses in the list is unknown
      // until we loop through the entire list and we figure out how
      // hash ids are per HotRod server (i.e. num virtual nodes > 1)
      val members = new ListBuffer[ServerAddress]()
      val allHashIds = mutable.Map.empty[ServerAddress, Seq[Int]]
      var hashIdsOfAddr = new ListBuffer[Int]()
      var prevNode: ServerAddress = null
      for (i <- 1 to numServersInTopo) {
         val node = new ServerAddress(readString(buf), readUnsignedShort(buf))
         val hashId = buf.readInt
         if (prevNode == null || node == prevNode) {
            // First time node has been seen, so cache it
            if (prevNode == null)
               prevNode = node

            // Add current hash id to list
            hashIdsOfAddr += hashId
         } else {
            // A new node has been detected, so create the topology
            // address and store it in the view
            allHashIds += (prevNode -> hashIdsOfAddr)
            members += prevNode
            prevNode = node
            hashIdsOfAddr = new ListBuffer[Int]()
            hashIdsOfAddr += hashId
         }
         // Check for last server hash in which case just add it
         if (i == numServersInTopo) {
            allHashIds += (prevNode -> hashIdsOfAddr)
            members += prevNode
         }

      }
      Some(TestHashDistAware10Response(topologyId, members.toList,
            immutable.Map[ServerAddress, Seq[Int]]() ++ allHashIds,
            numOwners, hashFunction, hashSpace))
   }

   private def read11HashDistAwareHeader(buf: ByteBuf, topologyId: Int,
            numOwners: Int, hashFunction: Byte, hashSpace: Int,
            numServersInTopo: Int): Option[AbstractTestTopologyAwareResponse] = {
      val numVirtualNodes = readUnsignedInt(buf)
      val hashToAddress = mutable.Map[ServerAddress, Int]()
      for (i <- 1 to numServersInTopo)
         hashToAddress += ((new ServerAddress(readString(buf), readUnsignedShort(buf)) -> buf.readInt()))

      Some(TestHashDistAware11Response(topologyId, immutable.Map[ServerAddress, Int]() ++ hashToAddress,
            numOwners, hashFunction, hashSpace, numVirtualNodes))
   }

}

private class ClientHandler(rspTimeoutSeconds: Int) extends ChannelInboundHandlerAdapter {

   private val responses = new ConcurrentHashMap[Long, TestResponse]
   private val clientListeners = new EquivalentConcurrentHashMapV8[Bytes, TestClientListener](
         ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance())

   def addClientListener(listener: TestClientListener): Unit =
      clientListeners.put(listener.getId, listener)

   def removeClientListener(listenerId: Bytes): Unit = clientListeners.remove(listenerId)

   override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef) {
      msg match {
         case e: TestKeyWithVersionEvent if e.operation == CacheEntryCreatedEventResponse =>
            clientListeners.get(e.listenerId).onCreated(e)
         case e: TestKeyWithVersionEvent if e.operation == CacheEntryModifiedEventResponse =>
            clientListeners.get(e.listenerId).onModified(e)
         case e: TestKeyEvent =>
            clientListeners.get(e.listenerId).onRemoved(e)
         case e: TestCustomEvent =>
            clientListeners.get(e.listenerId).onCustom(e)
         case resp: TestResponse =>
            trace("Put %s in responses", resp)
            responses.put(resp.messageId, resp)
      }
   }

   def getResponse(messageId: Long): TestResponse = {
      // TODO: Very very primitive way of waiting for a response. Convert to a Future
      var i = 0
      var v: TestResponse = null
      do {
         v = responses.get(messageId)
         if (v == null) {
            TestingUtil.sleepThread(100)
            i += 1
         }
      }
      while (v == null && i < (rspTimeoutSeconds * 10))
      v
   }

}

class Op(val magic: Int,
         val version: Byte,
         val code: Byte,
         val cacheName: String,
         val key: Array[Byte],
         val lifespan: Int,
         val maxIdle: Int,
         val value: Array[Byte],
         val flags: Int,
         val dataVersion: Long,
         val clientIntel: Byte,
         val topologyId: Int) {
   lazy val id = HotRodClient.idCounter.incrementAndGet
   override def toString = {
      new StringBuilder().append("Op").append("(")
         .append(id).append(',')
         .append(magic).append(',')
         .append(version).append(',')
         .append(code).append(',')
         .append(cacheName).append(',')
         .append(if (key == null) "null" else Util.printArray(key, true)).append(',')
         .append(maxIdle).append(',')
         .append(lifespan).append(',')
         .append(if (value == null) "null" else Util.printArray(value, true)).append(',')
         .append(flags).append(',')
         .append(dataVersion).append(',')
         .append(clientIntel).append(',')
         .append(topologyId).append(')')
         .toString
   }

}

class PartialOp(override val magic: Int,
                override val version: Byte,
                override val code: Byte,
                override val cacheName: String,
                override val key: Array[Byte],
                override val lifespan: Int,
                override val maxIdle: Int,
                override val value: Array[Byte],
                override val flags: Int,
                override val dataVersion: Long,
                override val clientIntel: Byte,
                override val topologyId: Int)
      extends Op(magic, version, code, cacheName, key, lifespan, maxIdle, value,
                 flags, dataVersion, clientIntel, topologyId)

class StatsOp(override val magic: Int,
              override val version: Byte,
              override val code: Byte,
              override val cacheName: String,
              override val clientIntel: Byte,
              override val topologyId: Int,
              val statName: String)
      extends Op(magic, version, code, cacheName, null, 0, 0, null, 0, 0,
                 clientIntel, topologyId)

class BulkGetOp(override val magic: Int,
              override val version: Byte,
              override val code: Byte,
              override val cacheName: String,
              override val clientIntel: Byte,
              override val topologyId: Int,
              val count: Int)
     extends Op(magic, version, code, cacheName, null, 0, 0, null, 0, 0,
                clientIntel, topologyId)

class BulkGetKeysOp(override val magic: Int,
        override val version: Byte,
        override val code: Byte,
        override val cacheName: String,
        override val clientIntel: Byte,
        override val topologyId: Int,
        val scope: Int)
        extends Op(magic, version, code, cacheName, null, 0, 0, null, 0, 0,
           clientIntel, topologyId)

class QueryOp(override val magic: Int,
        override val version: Byte,
        override val cacheName: String,
        override val clientIntel: Byte,
        override val topologyId: Int,
        val query: Array[Byte])
        extends Op(magic, version, 0x1F, cacheName, null, 0, 0, null, 0, 0,
           clientIntel, topologyId)

class AddClientListenerOp(override val magic: Int,
        override val version: Byte,
        override val cacheName: String,
        override val clientIntel: Byte,
        override val topologyId: Int,
        val listenerId: Bytes,
        val filterFactory: NamedFactory,
        val converterFactory: NamedFactory)
        extends Op(magic, version, 0x25, cacheName, null, 0, 0, null, 0, 0,
           clientIntel, topologyId)

class RemoveClientListenerOp(override val magic: Int,
        override val version: Byte,
        override val cacheName: String,
        override val clientIntel: Byte,
        override val topologyId: Int,
        val listenerId: Bytes)
        extends Op(magic, version, 0x27, cacheName, null, 0, 0, null, 0, 0,
           clientIntel, topologyId)

class AuthMechListOp(override val magic: Int,
              override val version: Byte,
              override val code: Byte,
              override val cacheName: String,
              override val clientIntel: Byte,
              override val topologyId: Int)
      extends Op(magic, version, code, cacheName, null, 0, 0, null, 0, 0,
                 clientIntel, topologyId)

class AuthOp(override val magic: Int,
              override val version: Byte,
              override val code: Byte,
              override val cacheName: String,
              override val clientIntel: Byte,
              override val topologyId: Int,
              val mech: String,
              val response: Array[Byte])
      extends Op(magic, version, code, cacheName, null, 0, 0, null, 0, 0,
                 clientIntel, topologyId)

class TestResponse(override val version: Byte, override val messageId: Long,
                   override val cacheName: String, override val clientIntel: Short,
                   override val operation: OperationResponse,
                   override val status: OperationStatus,
                   override val topologyId: Int,
                   val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends Response(version, messageId, cacheName, clientIntel, operation, status, topologyId) {
   override def toString = {
      new StringBuilder().append("Response").append("{")
         .append("version=").append(version)
         .append(", messageId=").append(messageId)
         .append(", operation=").append(operation)
         .append(", status=").append(status)
         .append(", cacheName=").append(cacheName)
         .append(", topologyResponse=").append(topologyResponse)
         .append("}").toString()
   }

   def asTopologyAwareResponse: AbstractTestTopologyAwareResponse = {
      topologyResponse.get match {
         case t: AbstractTestTopologyAwareResponse => t
         case _ => throw new IllegalStateException("Unexpected response: " + topologyResponse.get);
      }
   }
}

class TestResponseWithPrevious(override val version: Byte, override val messageId: Long,
                           override val cacheName: String, override val clientIntel: Short,
                           override val operation: OperationResponse,
                           override val status: OperationStatus,
                           override val topologyId: Int, val previous: Option[Array[Byte]],
                           override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse)

class TestGetResponse(override val version: Byte, override val messageId: Long,
                  override val cacheName: String, override val clientIntel: Short,
                  override val operation: OperationResponse, override val status: OperationStatus,
                  override val topologyId: Int, val data: Option[Array[Byte]],
                  override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse)

class TestGetWithVersionResponse(override val version: Byte, override val messageId: Long,
                             override val cacheName: String, override val clientIntel: Short,
                             override val operation: OperationResponse,
                             override val status: OperationStatus,
                             override val topologyId: Int,
                             override val data: Option[Array[Byte]], val dataVersion: Long,
                             override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestGetResponse(version, messageId, cacheName, clientIntel, operation, status, topologyId, data, topologyResponse)

class TestGetWithMetadataResponse(override val version: Byte, override val messageId: Long,
                             override val cacheName: String, override val clientIntel: Short,
                             override val operation: OperationResponse,
                             override val status: OperationStatus,
                             override val topologyId: Int,
                             override val data: Option[Array[Byte]], val dataVersion: Long,
                             val created: Long, val lifespan: Int, val lastUsed: Long, val maxIdle: Int,
                             override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestGetResponse(version, messageId, cacheName, clientIntel, operation, status, topologyId, data, topologyResponse)


class TestErrorResponse(override val version: Byte, override val messageId: Long,
                    override val cacheName: String, override val clientIntel: Short,
                    override val status: OperationStatus,
                    override val topologyId: Int, val msg: String,
                    override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, ErrorResponse, status, topologyId, topologyResponse)

class TestStatsResponse(override val version: Byte, override val messageId: Long,
                        override val cacheName: String, override val clientIntel: Short,
                        val stats: Map[String, String], override val topologyId: Int,
                        override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, StatsResponse, Success, topologyId, topologyResponse)

class TestBulkGetResponse(override val version: Byte, override val messageId: Long,
                          override val cacheName: String, override val clientIntel: Short,
                          val bulkData: Map[Array[Byte], Array[Byte]],
                          override val topologyId: Int, override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, BulkGetResponse, Success, topologyId, topologyResponse)

class TestBulkGetKeysResponse(override val version: Byte, override val messageId: Long,
                          override val cacheName: String, override val clientIntel: Short,
                          val bulkData: Set[Array[Byte]],
                          override val topologyId: Int, override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, BulkGetResponse, Success, topologyId, topologyResponse)

class TestQueryResponse(override val version: Byte, override val messageId: Long,
                          override val cacheName: String, override val clientIntel: Short,
                          val result: Array[Byte],
                          override val topologyId: Int, override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, QueryResponse, Success, topologyId, topologyResponse)

class TestAuthMechListResponse(override val version: Byte, override val messageId: Long,
                        override val cacheName: String, override val clientIntel: Short,
                        val mechs: Set[String], override val topologyId: Int,
                        override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, AuthMechListResponse, Success, topologyId, topologyResponse)

class TestAuthResponse(override val version: Byte, override val messageId: Long,
                        override val cacheName: String, override val clientIntel: Short,
                        val complete: Boolean, val challenge: Array[Byte], override val topologyId: Int,
                        override val topologyResponse: Option[AbstractTestTopologyAwareResponse])
      extends TestResponse(version, messageId, cacheName, clientIntel, AuthResponse, Success, topologyId, topologyResponse)

class TestKeyWithVersionEvent(override val version: Byte, override val messageId: Long,
        override val cacheName: String, override val operation: OperationResponse,
        val listenerId: Bytes, val key: Bytes, val dataVersion: Long)
        extends TestResponse(version, messageId, cacheName, 0, operation, Success, 0, None)

class TestKeyEvent(override val version: Byte, override val messageId: Long,
        override val cacheName: String, val listenerId: Bytes, val key: Bytes)
        extends TestResponse(version, messageId, cacheName, 0, CacheEntryRemovedEventResponse, Success, 0, None)

class TestCustomEvent(override val version: Byte, override val messageId: Long,
        override val cacheName: String, override val operation: OperationResponse,
        val listenerId: Bytes, val eventData: Bytes)
        extends TestResponse(version, messageId, cacheName, 0, operation, Success, 0, None)

case class ServerNode(host: String, port: Int)

abstract class AbstractTestTopologyAwareResponse(val topologyId: Int,
                                     val members: Iterable[ServerAddress])

case class TestTopologyAwareResponse(override val topologyId: Int,
                                         override val members : Iterable[ServerAddress])
      extends AbstractTestTopologyAwareResponse(topologyId, members)

case class TestHashDistAware10Response(override val topologyId: Int,
                        override val members: Iterable[ServerAddress],
                        hashIds: Map[ServerAddress, Seq[Int]],
                        numOwners: Int, hashFunction: Byte, hashSpace: Int)
      extends AbstractTestTopologyAwareResponse(topologyId, members)

case class TestHashDistAware11Response(override val topologyId: Int,
                        membersToHash: Map[ServerAddress, Int],
                        numOwners: Int, hashFunction: Byte, hashSpace: Int,
                        numVirtualNodes: Int)
      extends AbstractTestTopologyAwareResponse(topologyId, membersToHash.keys)

case class TestHashDistAware20Response(override val topologyId: Int,
        override val members: Iterable[ServerAddress],
        segments: Seq[Iterable[ServerAddress]],
        hashFunction: Byte)
        extends AbstractTestTopologyAwareResponse(topologyId, members)
