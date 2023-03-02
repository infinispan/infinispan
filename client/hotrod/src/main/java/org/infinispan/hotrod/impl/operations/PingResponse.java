package org.infinispan.hotrod.impl.operations;

import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.hotrod.configuration.ProtocolVersion;
import org.infinispan.hotrod.exceptions.HotRodClientException;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;

public class PingResponse {

   public static final PingResponse EMPTY = new PingResponse(null);

   private final short status;
   private final ProtocolVersion version;
   private final MediaType keyMediaType;
   private final MediaType valueMediaType;
   private final Throwable error;
   private final Set<Short> serverOps;

   public PingResponse(short status, ProtocolVersion version, MediaType keyMediaType, MediaType valueMediaType,
                       Set<Short> serverOps) {
      this.status = status;
      this.version = version;
      this.keyMediaType = keyMediaType;
      this.valueMediaType = valueMediaType;
      this.serverOps = serverOps;
      this.error = null;
   }

   PingResponse(Throwable error) {
      this.status = -1;
      this.version = ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
      this.keyMediaType = MediaType.APPLICATION_UNKNOWN;
      this.valueMediaType = MediaType.APPLICATION_UNKNOWN;
      this.serverOps = Collections.emptySet();
      this.error = error;
   }

   public short getStatus() {
      return status;
   }

   public boolean isSuccess() {
      return HotRodConstants.isSuccess(status);
   }

   public boolean isObjectStorage() {
      return keyMediaType != null && keyMediaType.match(MediaType.APPLICATION_OBJECT);
   }

   public boolean isFailed() {
      return error != null;
   }

   public boolean isCacheNotFound() {
      return error instanceof HotRodClientException && error.getMessage().contains("CacheNotFoundException");
   }

   public Set<Short> getServerOps() {
      return serverOps;
   }

   public ProtocolVersion getVersion() {
      return version;
   }

   public MediaType getKeyMediaType() {
      return keyMediaType;
   }

   public MediaType getValueMediaType() {
      return valueMediaType;
   }
}
