/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
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
package org.infinispan.server.hotrod.test

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import org.testng.Assert._
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.OperationResponse._
import collection.mutable
import collection.immutable
import java.lang.reflect.Method
import HotRodTestingUtil._
import java.util.concurrent.{ConcurrentHashMap, Executors}
import java.util.concurrent.atomic.{AtomicLong}
import mutable.ListBuffer
import org.infinispan.test.TestingUtil
import org.infinispan.util.Util
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import org.jboss.netty.handler.codec.replay.{VoidEnum, ReplayingDecoder}
import org.infinispan.server.hotrod._
import java.lang.IllegalStateException
import java.lang.StringBuilder
<<<<<<< HEAD
import javax.net.ssl.SSLEngine
import org.jboss.netty.handler.ssl.SslHandler
=======
import java.util
>>>>>>> ISPN-2281 Hot Rod uses byte[] as raw key type instead of ByteArrayKey

/**
 * A very simply Hot Rod client for testing purpouses. It's a quick and dirty client implementation done for testing
 * purpouses. As a result, it might not be very readable, particularly for readers not used to scala.
 *
 * Reasons why this should not really be a trait:
 * Storing var instances in a trait cause issues with TestNG, see:
 *   http://thread.gmane.org/gmane.comp.lang.scala.user/24317
 *
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 * @since 4.1
 */
class HotRodClient(host: String, port: Int, defaultCacheName: String, rspTimeoutSeconds: Int, protocolVersion: Byte, sslEngine: SSLEngine = null) extends Log {
   val idToOp = new ConcurrentHashMap[Long, Op]

   private lazy val ch: Channel = {
      val factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool)
      val bootstrap: ClientBootstrap = new ClientBootstrap(factory)
      bootstrap.setPipelineFactory(new ClientPipelineFactory(this, rspTimeoutSeconds, sslEngine))
      bootstrap.setOption("tcpNoDelay", true)
      bootstrap.setOption("keepAlive", true)
      // Make a new connection.
      val connectFuture = bootstrap.connect(new InetSocketAddress(host, port))
      // Wait until the connection is made successfully.
      val ch = connectFuture.awaitUninterruptibly.getChannel
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
      val future = ch.write(op)
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
      val handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse(expectedResponseMessageId)
   }

   private def writeOp(op: Op) {
      idToOp.put(op.id, op)
      val future = ch.write(op)
      future.awaitUninterruptibly
      assertTrue(future.isSuccess)
   }

   def get(k: Array[Byte], flags: Int): TestGetResponse = {
      get(0x03, k, 0).asInstanceOf[TestGetResponse]
   }

   def assertGet(m: Method): TestGetResponse = assertGet(m, 0)

   def assertGet(m: Method, flags: Int): TestGetResponse = get(k(m), flags)

   def containsKey(k: Array[Byte], flags: Int): TestResponse = {
      get(0x0F, k, 0)
   }

   def getWithVersion(k: Array[Byte], flags: Int): TestGetWithVersionResponse =
      get(0x11, k, 0).asInstanceOf[TestGetWithVersionResponse]

   def getWithMetadata(k: Array[Byte], flags: Int): TestGetWithMetadataResponse =
      get(0x1B, k, 0).asInstanceOf[TestGetWithMetadataResponse]

   private def get(code: Byte, k: Array[Byte], flags: Int): TestResponse = {
      val op = new Op(0xA0, protocolVersion, code, defaultCacheName, k, 0, 0, null, flags, 0, 1, 0)
      val writeFuture = writeOp(op)
      // Get the handler instance to retrieve the answer.
      val handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
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
      val handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
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
      val handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestBulkGetResponse]
   }

   def bulkGetKeys: TestBulkGetKeysResponse = {
   	  val op = new BulkGetKeysOp(0xA0, protocolVersion, 0x1D, defaultCacheName, 1, 0, 0)
      val writeFuture = writeOp(op)
      val handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestBulkGetKeysResponse]
   }

   def bulkGetKeys(scope: Int): TestBulkGetKeysResponse = {
   	  val op = new BulkGetKeysOp(0xA0, protocolVersion, 0x1D, defaultCacheName, 1, 0, scope)
      val writeFuture = writeOp(op)
      val handler = ch.getPipeline.getLast.asInstanceOf[ClientHandler]
      handler.getResponse(op.id).asInstanceOf[TestBulkGetKeysResponse]
   }
}

private class ClientPipelineFactory(client: HotRodClient, rspTimeoutSeconds: Int, sslEngine: SSLEngine) extends ChannelPipelineFactory {

   override def getPipeline = {
      val pipeline = Channels.pipeline
      if (sslEngine != null)
         pipeline.addLast("ssl", new SslHandler(sslEngine));
      pipeline.addLast("decoder", new Decoder(client))
      pipeline.addLast("encoder", new Encoder)
      pipeline.addLast("handler", new ClientHandler(rspTimeoutSeconds))
      pipeline
   }

}

private class Encoder extends OneToOneEncoder {

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: AnyRef) = {
      trace("Encode %s so that it's sent to the server", msg)
      msg match {
         case partial: PartialOp => {
            val buffer = dynamicBuffer
            buffer.writeByte(partial.magic.asInstanceOf[Byte]) // magic
            writeUnsignedLong(partial.id, buffer) // message id
            buffer.writeByte(partial.version) // version
            buffer.writeByte(partial.code) // opcode
            buffer
         }
         case op: Op => {
            val buffer = dynamicBuffer
            buffer.writeByte(op.magic.asInstanceOf[Byte]) // magic
            writeUnsignedLong(op.id, buffer) // message id
            buffer.writeByte(op.version) // version
            buffer.writeByte(op.code) // opcode
            if (!op.cacheName.isEmpty) {
               writeRangedBytes(op.cacheName.getBytes(), buffer) // cache name length + cache name
            } else {
               writeUnsignedInt(0, buffer) // Zero length
            }
            writeUnsignedInt(op.flags, buffer) // flags
            buffer.writeByte(op.clientIntel) // client intelligence
            writeUnsignedInt(op.topologyId, buffer) // topology id
            writeRangedBytes(new Array[Byte](0), buffer)
            if (op.code != 0x13 && op.code != 0x15 && op.code != 0x17 && op.code != 0x19 && op.code != 0x1D) { // if it's a key based op...
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
            }
            buffer
         }
      }
   }

}

object HotRodClient {
   val idCounter = new AtomicLong
}

private class Decoder(client: HotRodClient) extends ReplayingDecoder[VoidEnum] with Log with Constants {

   override def decode(ctx: ChannelHandlerContext, ch: Channel, buf: ChannelBuffer, state: VoidEnum): Object = {
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
            if (op.flags == 1) {
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
         case ContainsKeyResponse | ClearResponse | PingResponse =>
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
         case ErrorResponse => {
            if (op == null)
               new TestErrorResponse(10, id, "", 0, status, 0,
                     readString(buf), topologyChangeResponse)
            else
               new TestErrorResponse(op.version, id, op.cacheName, op.clientIntel,
                     status, op.topologyId, readString(buf), topologyChangeResponse)
         }

      }
      trace("Got response from server: %s", resp)
      resp
   }

   override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      logExceptionReported(e.getCause)
   }

   private def read10HashDistAwareHeader(buf: ChannelBuffer, topologyId: Int,
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

   private def read11HashDistAwareHeader(buf: ChannelBuffer, topologyId: Int,
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

private class ClientHandler(rspTimeoutSeconds: Int) extends SimpleChannelUpstreamHandler {

   private val responses = new ConcurrentHashMap[Long, TestResponse]

   override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val resp = e.getMessage.asInstanceOf[TestResponse]
      trace("Put %s in responses", resp)
      responses.put(resp.messageId, resp)
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

case class ServerNode(val host: String, val port: Int)

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
                        val membersToHash: Map[ServerAddress, Int],
                        numOwners: Int, hashFunction: Byte, hashSpace: Int,
                        numVirtualNodes: Int)
      extends AbstractTestTopologyAwareResponse(topologyId, membersToHash.keys)
