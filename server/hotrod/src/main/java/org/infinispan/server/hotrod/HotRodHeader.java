package org.infinispan.server.hotrod;

import org.infinispan.commons.dataconversion.MediaType;

/**
 * @author wburns
 * @since 9.0
 */
public class HotRodHeader {
   HotRodOperation op;
   byte version;
   long messageId;
   String cacheName;
   int flag;
   short clientIntel;
   int topologyId;
   MediaType keyType;
   MediaType valueType;

   public HotRodOperation getOp() {
      return op;
   }

   public MediaType getKeyMediaType() {
      return keyType == null ? MediaType.MATCH_ALL : keyType;
   }

   public MediaType getValueMediaType() {
      return valueType == null ? MediaType.MATCH_ALL : valueType;
   }

   public byte getVersion() {
      return version;
   }

   public long getMessageId() {
      return messageId;
   }

   public String getCacheName() {
      return cacheName;
   }

   public int getFlag() {
      return flag;
   }

   public short getClientIntel() {
      return clientIntel;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "HotRodHeader{" +
            "op=" + op +
            ", version=" + version +
            ", messageId=" + messageId +
            ", cacheName='" + cacheName + '\'' +
            ", flag=" + flag +
            ", clientIntel=" + clientIntel +
            ", topologyId=" + topologyId +
            ", keyType=" + keyType +
            ", valueType=" + valueType +
            '}';
   }
}
