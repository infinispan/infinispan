package org.infinispan.server.hotrod;

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

   public HotRodOperation getOp() {
      return op;
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
            '}';
   }
}
