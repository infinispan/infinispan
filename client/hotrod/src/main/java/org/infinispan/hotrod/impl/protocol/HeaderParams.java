package org.infinispan.hotrod.impl.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.configuration.ClientIntelligence;

/**
 * Hot Rod request header parameters
 *
 * @since 14.0
 */
public class HeaderParams {
   final short opCode;
   final short opRespCode;
   byte[] cacheName;
   int flags;
   byte clientIntel;
   byte txMarker;
   AtomicInteger topologyId;
   final long messageId;
   int topologyAge;
   DataFormat dataFormat;
   Map<String, byte[]> otherParams;

   public HeaderParams(short requestCode, short responseCode, long messageId) {
      opCode = requestCode;
      opRespCode = responseCode;
      this.messageId = messageId;
   }

   public HeaderParams cacheName(byte[] cacheName) {
      this.cacheName = cacheName;
      return this;
   }

   public HeaderParams flags(int flags) {
      this.flags = flags;
      return this;
   }

   public HeaderParams clientIntel(ClientIntelligence clientIntel) {
      this.clientIntel = clientIntel.getValue();
      return this;
   }

   public HeaderParams txMarker(byte txMarker) {
      this.txMarker = txMarker;
      return this;
   }

   public long messageId() {
      return messageId;
   }

   public HeaderParams topologyId(AtomicInteger topologyId) {
      this.topologyId = topologyId;
      return this;
   }

   public AtomicInteger topologyId() {
      return topologyId;
   }

   public HeaderParams topologyAge(int topologyAge) {
      this.topologyAge = topologyAge;
      return this;
   }

   public HeaderParams dataFormat(DataFormat dataFormat) {
      this.dataFormat = dataFormat;
      return this;
   }

   public void otherParam(String paramKey, byte[] paramValue) {
      if (otherParams == null) {
         otherParams = new HashMap<>(2);
      }
      otherParams.put(paramKey, paramValue);
   }
}
