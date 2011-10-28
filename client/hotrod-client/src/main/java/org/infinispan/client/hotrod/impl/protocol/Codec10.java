/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.util.Util.hexDump;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspecException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * A Hot Rod encoder/decoder for version 1.0 of the protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class Codec10 implements Codec {

   private static final Log log = LogFactory.getLog(Codec10.class, Log.class);

   static final AtomicLong MSG_ID = new AtomicLong();

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_10);
   }

   protected HeaderParams writeHeader(
            Transport transport, HeaderParams params, byte version) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      transport.writeVLong(params.messageId(MSG_ID.incrementAndGet()).messageId);
      transport.writeByte(version);
      transport.writeByte(params.opCode);
      transport.writeArray(params.cacheName);

      int flagInt = 0;
      if (params.flags != null) {
         for (Flag flag : params.flags) {
            flagInt = flag.getFlagInt() | flagInt;
         }
      }
      transport.writeVInt(flagInt);
      transport.writeByte(params.clientIntel);
      transport.writeVInt(params.topologyId.get());
      //todo change once TX support is added
      transport.writeByte(params.txMarker);
      log.tracef("Wrote header for message %d. Operation code: %#04x. Flags: %#x",
                 params.messageId, params.opCode, flagInt);
      return params;
   }


   public short readHeader(Transport transport, HeaderParams params) {
      short magic = transport.readByte();
      boolean isTrace = log.isTraceEnabled();
      if (magic != HotRodConstants.RESPONSE_MAGIC) {
         String message = "Invalid magic number. Expected %#x and received %#x";
         log.invalidMagicNumber(HotRodConstants.RESPONSE_MAGIC, magic);
         if (isTrace)
            log.tracef("Socket dump: %s", hexDump(transport.dumpStream()));
         throw new InvalidResponseException(String.format(message, HotRodConstants.RESPONSE_MAGIC, magic));
      }
      long receivedMessageId = transport.readVLong();
      if (receivedMessageId != params.messageId) {
         String message = "Invalid message id. Expected %d and received %d";
         log.invalidMessageId(params.messageId, receivedMessageId);
         if (isTrace)
            log.tracef("Socket dump: %s", hexDump(transport.dumpStream()));
         throw new InvalidResponseException(String.format(message, params.messageId, receivedMessageId));
      }
      log.tracef("Received response for message id: %d", receivedMessageId);

      short receivedOpCode = transport.readByte();
      if (receivedOpCode != params.opRespCode) {
         if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(transport, params, transport.readByte());
         }
         throw new InvalidResponseException(String.format(
               "Invalid response operation. Expected %#x and received %#x",
               params.opRespCode, receivedOpCode));
      }
      log.tracef("Received operation code is: %#04x", receivedOpCode);

      short status = transport.readByte();
      // There's not need to check for errors in status here because if there
      // was an error, the server would have replied with error response op code.
      readNewTopologyIfPresent(transport, params);
      return status;
   }

   private void checkForErrorsInResponseStatus(Transport transport, HeaderParams params, short status) {
      final boolean isTrace = log.isTraceEnabled();
      if (isTrace) log.tracef("Received operation status: %#x", status);

      switch ((int) status) {
         case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
         case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
         case HotRodConstants.UNKNOWN_COMMAND_STATUS:
         case HotRodConstants.SERVER_ERROR_STATUS:
         case HotRodConstants.COMMAND_TIMEOUT_STATUS:
         case HotRodConstants.UNKNOWN_VERSION_STATUS: {
            readNewTopologyIfPresent(transport, params);
            String msgFromServer = transport.readString();
            if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && isTrace) {
               log.tracef("Server-side timeout performing operation: %s", msgFromServer);
            } if (msgFromServer.contains("SuspectException")) {
               if (isTrace)
                  log.tracef("A remote node was suspected while executing messageId=%d. " +
                     "Check if retry possible. Message from server: %s", params.messageId, msgFromServer);
               // TODO: This will be better handled with its own status id in version 2 of protocol
               throw new RemoteNodeSuspecException(msgFromServer, params.messageId, status);
            } else {
               log.errorFromServer(msgFromServer);
            }
            throw new HotRodClientException(msgFromServer, params.messageId, status);
         }
         default: {
            throw new IllegalStateException(String.format("Unknown status: %#04x", status));
         }
      }
   }

   private void readNewTopologyIfPresent(Transport transport, HeaderParams params) {
      short topologyChangeByte = transport.readByte();
      if (topologyChangeByte == 1)
         readNewTopologyAndHash(transport, params.topologyId);
   }

   protected void readNewTopologyAndHash(Transport transport, AtomicInteger topologyId) {
      int newTopologyId = transport.readVInt();
      topologyId.set(newTopologyId);
      int numKeyOwners = transport.readUnsignedShort();
      short hashFunctionVersion = transport.readByte();
      int hashSpace = transport.readVInt();
      int clusterSize = transport.readVInt();

      log.tracef("Topology change request: newTopologyId=%d, numKeyOwners=%d, " +
                       "hashFunctionVersion=%d, hashSpaceSize=%d, clusterSize=%d",
                 newTopologyId, numKeyOwners, hashFunctionVersion, hashSpace, clusterSize);

      Map<SocketAddress, Set<Integer>> servers2Hash = new LinkedHashMap<SocketAddress, Set<Integer>>();

      for (int i = 0; i < clusterSize; i++) {
         String host = transport.readString();
         int port = transport.readUnsignedShort();
         log.tracef("Server read: %s:%d", host, port);
         int hashCode = transport.read4ByteInt();
         InetSocketAddress address = new InetSocketAddress(host, port);
         Set<Integer> hashes = servers2Hash.get(address);
         if (hashes == null) {
            hashes = new HashSet<Integer>();
            servers2Hash.put(address, hashes);
         }
         hashes.add(hashCode);
         log.tracef("Hash code is: %d", hashCode);
      }

      if (log.isInfoEnabled()) {
         log.newTopology(servers2Hash.keySet());
      }
      transport.getTransportFactory().updateServers(servers2Hash.keySet());
      if (hashFunctionVersion == 0) {
         log.trace("Not using a consistent hash function (hash function version == 0).");
      } else {
         transport.getTransportFactory().updateHashFunction(servers2Hash, numKeyOwners, hashFunctionVersion, hashSpace);
      }
   }

}
