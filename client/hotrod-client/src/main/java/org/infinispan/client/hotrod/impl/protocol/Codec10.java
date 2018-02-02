package org.infinispan.client.hotrod.impl.protocol;

import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Either;

import io.netty.buffer.ByteBuf;

/**
 * A Hot Rod encoder/decoder for version 1.0 of the protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class Codec10 implements Codec {

   private static final Log log = LogFactory.getLog(Codec10.class, Log.class);
   protected final boolean trace = getLog().isTraceEnabled();

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_10);
   }

   @Override
   public void writeClientListenerParams(ByteBuf buf, ClientListener clientListener,
                                         byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      // No-op
   }

   @Override
   public void writeExpirationParams(ByteBuf buf, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      if (!CodecUtils.isIntCompatible(lifespan)) {
         getLog().warn("Lifespan value greater than the max supported size (Integer.MAX_VALUE), this can cause precision loss");
      }
      if (!CodecUtils.isIntCompatible(maxIdle)) {
         getLog().warn("MaxIdle value greater than the max supported size (Integer.MAX_VALUE), this can cause precision loss");
      }
      int lifespanSeconds = CodecUtils.toSeconds(lifespan, lifespanTimeUnit);
      int maxIdleSeconds = CodecUtils.toSeconds(maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeVInt(buf, lifespanSeconds);
      ByteBufUtil.writeVInt(buf, maxIdleSeconds);
   }

   @Override
   public int estimateExpirationSize(long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      int lifespanSeconds = CodecUtils.toSeconds(lifespan, lifespanTimeUnit);
      int maxIdleSeconds = CodecUtils.toSeconds(maxIdle, maxIdleTimeUnit);
      return ByteBufUtil.estimateVIntSize(lifespanSeconds) + ByteBufUtil.estimateVIntSize(maxIdleSeconds);
   }

   protected HeaderParams writeHeader(
         ByteBuf buf, HeaderParams params, byte version) {
      buf.writeByte(HotRodConstants.REQUEST_MAGIC);
      ByteBufUtil.writeVLong(buf, params.messageId);
      buf.writeByte(version);
      buf.writeByte(params.opCode);
      ByteBufUtil.writeArray(buf, params.cacheName);

      int flagInt = params.flags & Flag.FORCE_RETURN_VALUE.getFlagInt(); // 1.0 / 1.1 servers only understand this flag
      ByteBufUtil.writeVInt(buf, flagInt);
      buf.writeByte(params.clientIntel);
      ByteBufUtil.writeVInt(buf, params.topologyId.get());
      //todo change once TX support is added
      buf.writeByte(params.txMarker);
      if (trace) getLog().tracef("Wrote header for message %d. Operation code: %#04x. Flags: %#x",
         params.messageId, params.opCode, flagInt);
      return params;
   }

   @Override
   public int estimateHeaderSize(HeaderParams headerParams) {
      return 1 + ByteBufUtil.estimateVLongSize(headerParams.messageId) + 1 + 1 +
            ByteBufUtil.estimateArraySize(headerParams.cacheName) +
            ByteBufUtil.estimateVIntSize(headerParams.flags) + 1 +
            ByteBufUtil.estimateVIntSize(headerParams.topologyId.get()) + 1;
   }

   @Override
   public long readMessageId(ByteBuf buf) {
      short magic = buf.readUnsignedByte();
      final Log localLog = getLog();
      if (magic != HotRodConstants.RESPONSE_MAGIC) {
         String message = "Invalid magic number. Expected %#x and received %#x";
         localLog.invalidMagicNumber(HotRodConstants.RESPONSE_MAGIC, magic);
         if (trace)
            localLog.tracef("Socket dump: %s", ByteBufUtil.hexDump(buf));
         throw new InvalidResponseException(String.format(message, HotRodConstants.RESPONSE_MAGIC, magic));
      }
      long receivedMessageId = ByteBufUtil.readVLong(buf);
      if (trace) {
         getLog().tracef("Received response for messageId=%d", receivedMessageId);
      }
      return receivedMessageId;
   }

   @Override
   public short readHeader(ByteBuf buf, HeaderParams params, ChannelFactory channelFactory, SocketAddress serverAddress) {
      short receivedOpCode = buf.readUnsignedByte();
      // Read both the status and new topology (if present),
      // before deciding how to react to error situations.
      short status = buf.readUnsignedByte();
      readNewTopologyIfPresent(buf, params, channelFactory);

      // Now that all headers values have been read, check the error responses.
      // This avoids situatiations where an exceptional return ends up with
      // the socket containing data from previous request responses.
      if (receivedOpCode != params.opRespCode) {
         if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(buf, params, status);
         }
         throw new InvalidResponseException(String.format(
               "Invalid response operation. Expected %#x and received %#x",
               params.opRespCode, receivedOpCode));
      }
      if (trace) getLog().tracef("Received operation code is: %#04x", receivedOpCode);

      return status;
   }

   @Override
   public ClientEvent readEvent(ByteBuf buf, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist, SocketAddress serverAddress) {
      return null;  // No events sent in Hot Rod 1.x protocol
   }

   @Override
   public Either<Short, ClientEvent> readHeaderOrEvent(ByteBuf buf, HeaderParams params, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist, ChannelFactory channelFactory, SocketAddress serverAddress) {
      return null;  // No events sent in Hot Rod 1.x protocol
   }

   @Override
   public HotRodCounterEvent readCounterEvent(ByteBuf buf, byte[] listenerId) {
      return null;  // No events sent in Hot Rod 1.x protocol
   }

   @Override
   public Object returnPossiblePrevValue(ByteBuf buf, short status, int flags, List<String> whitelist, Marshaller marshaller) {
      if (hasForceReturn(flags)) {
         return CodecUtils.readUnmarshallByteArray(buf, status, whitelist, marshaller);
      } else {
         return null;
      }
   }

   private boolean hasForceReturn(int flags) {
      return (flags & Flag.FORCE_RETURN_VALUE.getFlagInt()) != 0;
   }

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   public <T> T readUnmarshallByteArray(ByteBuf buf, short status, List<String> whitelist, Marshaller marshaller) {
      return CodecUtils.readUnmarshallByteArray(buf, status, whitelist, marshaller);
   }

   public void writeClientListenerInterests(ByteBuf buf, Set<Class<? extends Annotation>> classes) {
      // No-op
   }

   protected void checkForErrorsInResponseStatus(ByteBuf buf, HeaderParams params, short status) {
      final Log localLog = getLog();
      if (trace) localLog.tracef("Received operation status: %#x", status);

      try {
         switch (status) {
            case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
            case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
            case HotRodConstants.UNKNOWN_COMMAND_STATUS:
            case HotRodConstants.SERVER_ERROR_STATUS:
            case HotRodConstants.COMMAND_TIMEOUT_STATUS:
            case HotRodConstants.UNKNOWN_VERSION_STATUS: {
               // If error, the body of the message just contains a message
               String msgFromServer = ByteBufUtil.readString(buf);
               if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && trace) {
                  localLog.tracef("Server-side timeout performing operation: %s", msgFromServer);
               } if (msgFromServer.contains("SuspectException")
                     || msgFromServer.contains("SuspectedException")) {
                  // Handle both Infinispan's and JGroups' suspicions
                  if (trace)
                     localLog.tracef("A remote node was suspected while executing messageId=%d. " +
                        "Check if retry possible. Message from server: %s", params.messageId, msgFromServer);
                  // TODO: This will be better handled with its own status id in version 2 of protocol
                  throw new RemoteNodeSuspectException(msgFromServer, params.messageId, status);
               } else {
                  localLog.errorFromServer(msgFromServer);
               }
               throw new HotRodClientException(msgFromServer, params.messageId, status);
            }
            default: {
               throw new IllegalStateException(String.format("Unknown status: %#04x", status));
            }
         }
      } finally {
         // Errors related to protocol parsing are odd, and they can sometimes
         // be the consequence of previous errors, so whenever these errors
         // occur, invalidate the underlying transport instance so that a
         // brand new connection is established next time around.
         switch (status) {
            case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
            case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
            case HotRodConstants.UNKNOWN_COMMAND_STATUS:
            case HotRodConstants.UNKNOWN_VERSION_STATUS: {
               // the channel will be invalidated in operation upon exception thrown above
            }
         }
      }
   }

   protected void readNewTopologyIfPresent(ByteBuf buf, HeaderParams params, ChannelFactory channelFactory) {
      short topologyChangeByte = buf.readUnsignedByte();
      if (topologyChangeByte == 1)
         readNewTopologyAndHash(buf, params.topologyId, params.cacheName, channelFactory);
   }

   protected void readNewTopologyAndHash(ByteBuf buf, AtomicInteger topologyId, byte[] cacheName,
                                         ChannelFactory channelFactory) {
      final Log localLog = getLog();
      int newTopologyId = ByteBufUtil.readVInt(buf);
      topologyId.set(newTopologyId);
      int numKeyOwners = buf.readUnsignedShort();
      short hashFunctionVersion = buf.readUnsignedByte();
      int hashSpace = ByteBufUtil.readVInt(buf);
      int clusterSize = ByteBufUtil.readVInt(buf);

      Map<SocketAddress, Set<Integer>> servers2Hash = computeNewHashes(
            buf, channelFactory, localLog, newTopologyId, numKeyOwners,
            hashFunctionVersion, hashSpace, clusterSize);

      Set<SocketAddress> socketAddresses = servers2Hash.keySet();
      int topologyAge = channelFactory.getTopologyAge();
      if (localLog.isInfoEnabled()) {
         localLog.newTopology(newTopologyId, topologyAge,
               socketAddresses.size(), socketAddresses);
      }
      channelFactory.updateServers(socketAddresses, cacheName, false);
      if (hashFunctionVersion == 0) {
         localLog.trace("Not using a consistent hash function (version 0).");
      } else if (hashFunctionVersion == 1) {
         localLog.trace("Ignoring obsoleted consistent hash function (version 1)");
      } else {
         channelFactory.updateHashFunction(
               servers2Hash, numKeyOwners, hashFunctionVersion, hashSpace, cacheName, topologyId);
      }
   }

   protected Map<SocketAddress, Set<Integer>> computeNewHashes(ByteBuf buf, ChannelFactory channelFactory,
                                                               Log localLog, int newTopologyId, int numKeyOwners,
                                                               short hashFunctionVersion, int hashSpace, int clusterSize) {
      if (trace) {
         localLog.tracef("Topology change request: newTopologyId=%d, numKeyOwners=%d, " +
                       "hashFunctionVersion=%d, hashSpaceSize=%d, clusterSize=%d",
                 newTopologyId, numKeyOwners, hashFunctionVersion, hashSpace, clusterSize);
      }

      Map<SocketAddress, Set<Integer>> servers2Hash = new LinkedHashMap<SocketAddress, Set<Integer>>();

      for (int i = 0; i < clusterSize; i++) {
         String host = ByteBufUtil.readString(buf);
         int port = buf.readUnsignedShort();
         int hashCode = buf.readIntLE();
         if (trace) localLog.tracef("Server read: %s:%d - hash code is %d", host, port, hashCode);
         SocketAddress address = InetSocketAddress.createUnresolved(host, port);
         Set<Integer> hashes = servers2Hash.get(address);
         if (hashes == null) {
            hashes = new HashSet<>();
            servers2Hash.put(address, hashes);
         }
         hashes.add(hashCode);
         if (trace) localLog.tracef("Hash code is: %d", hashCode);
      }
      return servers2Hash;
   }

}
