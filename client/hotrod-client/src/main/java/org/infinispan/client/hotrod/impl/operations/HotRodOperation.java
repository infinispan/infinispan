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
package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspecException;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.util.Util.hexDump;

/**
 * Generic Hot Rod operation. It is aware of {@link org.infinispan.client.hotrod.Flag}s and it is targeted against a
 * cache name. This base class encapsulates the knowledge of writing and reading a header, as described in the
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class HotRodOperation implements HotRodConstants {

   static final AtomicLong MSG_ID = new AtomicLong();

   private static final Log log = LogFactory.getLog(HotRodOperation.class, Log.class);

   protected final Flag[] flags;

   protected final byte[] cacheName;

   protected final AtomicInteger topologyId;
   
   private static final byte NO_TX = 0;
   private static final byte XA_TX = 1;


   protected HotRodOperation(Flag[] flags, byte[] cacheName, AtomicInteger topologyId) {
      this.flags = flags;
      this.cacheName = cacheName;
      this.topologyId = topologyId;
   }

   public abstract Object execute();

   protected final long writeHeader(Transport transport, short operationCode) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      long messageId = MSG_ID.incrementAndGet();
      transport.writeVLong(messageId);
      transport.writeByte(HotRodConstants.HOTROD_VERSION);
      transport.writeByte(operationCode);
      transport.writeArray(cacheName);

      int flagInt = 0;
      if (flags != null) {
         for (Flag flag : flags) {
            flagInt = flag.getFlagInt() | flagInt;
         }
      }
      transport.writeVInt(flagInt);
      transport.writeByte(CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE);
      transport.writeVInt(topologyId.get());
      //todo change once TX support is added
      transport.writeByte(NO_TX);
      if (log.isTraceEnabled()) {
         log.tracef("wrote header for message %d. Operation code: %#04x. Flags: %#x",
                    messageId, operationCode, flagInt);
      }
      return messageId;
   }

   /**
    * Magic	| Message Id | Op code | Status | Topology Change Marker
    */
   protected short readHeaderAndValidate(Transport transport, long messageId, short opRespCode) {
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
      if (receivedMessageId != messageId) {
         String message = "Invalid message id. Expected %d and received %d";
         log.invalidMessageId(messageId, receivedMessageId);
         if (isTrace)
            log.tracef("Socket dump: %s", hexDump(transport.dumpStream()));
         throw new InvalidResponseException(String.format(message, messageId, receivedMessageId));
      }
      if (isTrace) {
         log.tracef("Received response for message id: %d", receivedMessageId);
      }
      short receivedOpCode = transport.readByte();
      if (receivedOpCode != opRespCode) {
         if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(transport.readByte(), messageId, transport);
         }
         throw new InvalidResponseException(String.format(
               "Invalid response operation. Expected %#x and received %#x",
               opRespCode, receivedOpCode));
      }
      if (isTrace) {
         log.tracef("Received operation code is: %#04x", receivedOpCode);
      }
      short status = transport.readByte();
      // There's not need to check for errors in status here because if there
      // was an error, the server would have replied with error response op code.
      readNewTopologyIfPresent(transport);
      return status;
   }

   protected void checkForErrorsInResponseStatus(short status, long messageId, Transport transport) {
      final boolean isTrace = log.isTraceEnabled();
      if (isTrace) log.tracef("Received operation status: %#x", status);

      switch ((int) status) {
         case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
         case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
         case HotRodConstants.UNKNOWN_COMMAND_STATUS:
         case HotRodConstants.SERVER_ERROR_STATUS:
         case HotRodConstants.COMMAND_TIMEOUT_STATUS:
         case HotRodConstants.UNKNOWN_VERSION_STATUS: {
            readNewTopologyIfPresent(transport);
            String msgFromServer = transport.readString();
            if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && isTrace) {
               log.tracef("Server-side timeout performing operation: %s", msgFromServer);
            } if (msgFromServer.contains("SuspectException")) {
               if (isTrace)
                  log.tracef("A remote node was suspected while executing messageId=%d. " +
                     "Check if retry possible. Message from server: %s", messageId, msgFromServer);
               // TODO: This will be better handled with its own status id in version 2 of protocol
               throw new RemoteNodeSuspecException(msgFromServer, messageId, status);
            } else {
               log.errorFromServer(msgFromServer);
            }
            throw new HotRodClientException(msgFromServer, messageId, status);
         }
         default: {
            throw new IllegalStateException(String.format("Unknown status: %#04x", status));
         }
      }
   }

   private void readNewTopologyIfPresent(Transport transport) {
      short topologyChangeByte = transport.readByte();
      if (topologyChangeByte == 1)
         readNewTopologyAndHash(transport, topologyId);
   }

   private void readNewTopologyAndHash(Transport transport, AtomicInteger topologyId) {
      int newTopologyId = transport.readVInt();
      topologyId.set(newTopologyId);
      int numKeyOwners = transport.readUnsignedShort();
      short hashFunctionVersion = transport.readByte();
      int hashSpace = transport.readVInt();
      int clusterSize = transport.readVInt();

      boolean trace = log.isTraceEnabled();
      if (trace) {
         log.tracef("Topology change request: newTopologyId=%d, numKeyOwners=%d, " +
                          "hashFunctionVersion=%d, hashSpaceSize=%d, clusterSize=%d",
                    newTopologyId, numKeyOwners, hashFunctionVersion, hashSpace, clusterSize);
      }

      Map<SocketAddress, Set<Integer>> servers2Hash = new LinkedHashMap<SocketAddress, Set<Integer>>();

      for (int i = 0; i < clusterSize; i++) {
         String host = transport.readString();
         int port = transport.readUnsignedShort();
         if (trace) {
            log.tracef("Server read: %s:%d", host, port);
         }
         int hashCode = transport.read4ByteInt();
         InetSocketAddress address = new InetSocketAddress(host, port);
         Set<Integer> hashes = servers2Hash.get(address);
         if (hashes == null) {
            hashes = new HashSet<Integer>();
            servers2Hash.put(address, hashes);
         }
         hashes.add(hashCode);
         if (trace) {
            log.tracef("Hash code is: %d", hashCode);
         }
      }

      if (log.isInfoEnabled()) {
         log.newTopology(servers2Hash.keySet());
      }
      transport.getTransportFactory().updateServers(servers2Hash.keySet());
      if (hashFunctionVersion == 0) {
         if (trace)
            log.trace("Not using a consistent hash function (hash function version == 0).");
      } else {
         transport.getTransportFactory().updateHashFunction(servers2Hash, numKeyOwners, hashFunctionVersion, hashSpace);
      }
   }
}
