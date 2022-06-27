package org.infinispan.server.hotrod;

import java.util.EnumSet;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.Flag;
import org.infinispan.server.hotrod.logging.Log;

/**
 * @author wburns
 * @since 9.0
 */
public class HotRodHeader {
   private static final Log log = LogFactory.getLog(HotRodHeader.class, Log.class);

   HotRodOperation op;
   byte version;
   long messageId;
   String cacheName;
   int flag;
   short clientIntel;
   int topologyId;
   MediaType keyType;
   MediaType valueType;
   Map<String, byte[]> otherParams;

   public HotRodHeader(HotRodHeader header) {
      this(header.op, header.version, header.messageId, header.cacheName, header.flag, header.clientIntel, header.topologyId, header.keyType, header.valueType, header.otherParams);
   }

   public HotRodHeader(HotRodOperation op, byte version, long messageId, String cacheName, int flag, short clientIntel, int topologyId, MediaType keyType, MediaType valueType, Map<String, byte[]> otherParams) {
      this.op = op;
      this.version = version;
      this.messageId = messageId;
      this.cacheName = cacheName;
      this.flag = flag;
      this.clientIntel = clientIntel;
      this.topologyId = topologyId;
      this.keyType = keyType;
      this.valueType = valueType;
      this.otherParams = otherParams;
   }

   public boolean hasFlag(ProtocolFlag f) {
      return (flag & f.getValue()) == f.getValue();
   }

   public HotRodOperation getOp() {
      return op;
   }

   public MediaType getKeyMediaType() {
      return keyType == null ? MediaType.APPLICATION_UNKNOWN : keyType;
   }

   public MediaType getValueMediaType() {
      return valueType == null ? MediaType.APPLICATION_UNKNOWN : valueType;
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

   public VersionedEncoder encoder() {
      return HotRodVersion.getEncoder(version);
   }

   boolean isSkipCacheLoad() {
      if (version < 20) {
         return false;
      } else {
         return op.canSkipCacheLoading() && hasFlag(ProtocolFlag.SkipCacheLoader);
      }
   }

   boolean isSkipIndexing() {
      if (version < 20) {
         return false;
      } else {
         return op.canSkipIndexing() && hasFlag(ProtocolFlag.SkipIndexing);
      }
   }

   AdvancedCache<byte[], byte[]> getOptimizedCache(AdvancedCache<byte[], byte[]> c,
                                                   boolean transactional, boolean clustered) {
      EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
      if (hasFlag(ProtocolFlag.SkipListenerNotification)) {
         flags.add(Flag.SKIP_LISTENER_NOTIFICATION);
      }

      if (version < 20) {
         if (!hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
            switch (op) {
               case PUT:
               case PUT_IF_ABSENT:
                  flags.add(Flag.IGNORE_RETURN_VALUES);
            }
         }
         return c.withFlags(flags);
      }

      if (clustered && !transactional && op.isConditional()) {
         log.warnConditionalOperationNonTransactional(op.toString());
      }

      if (op.canSkipCacheLoading() && hasFlag(ProtocolFlag.SkipCacheLoader)) {
         flags.add(Flag.SKIP_CACHE_LOAD);
      }

      if (op.canSkipIndexing() && hasFlag(ProtocolFlag.SkipIndexing)) {
         flags.add(Flag.SKIP_INDEXING);
      }
      if (!hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
         if (op.isNotConditionalAndCanReturnPrevious()) {
            flags.add(Flag.IGNORE_RETURN_VALUES);
         }
      } else if (!transactional && op.canReturnPreviousValue()) {
         log.warnForceReturnPreviousNonTransactional(op.toString());
      }
      return c.withFlags(flags);
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
            ", otherParams=" + otherParams +
            '}';
   }
}
