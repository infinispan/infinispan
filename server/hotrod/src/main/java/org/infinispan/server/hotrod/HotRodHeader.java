package org.infinispan.server.hotrod;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
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

   public HotRodHeader(HotRodHeader header) {
      this(header.op, header.version, header.messageId, header.cacheName, header.flag, header.clientIntel, header.topologyId, header.keyType, header.valueType);
   }

   public HotRodHeader(HotRodOperation op, byte version, long messageId, String cacheName, int flag, short clientIntel, int topologyId, MediaType keyType, MediaType valueType) {
      this.op = op;
      this.version = version;
      this.messageId = messageId;
      this.cacheName = cacheName;
      this.flag = flag;
      this.clientIntel = clientIntel;
      this.topologyId = topologyId;
      this.keyType = keyType;
      this.valueType = valueType;
   }

   public boolean hasFlag(ProtocolFlag f) {
      return (flag & f.getValue()) == f.getValue();
   }

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

   AdvancedCache<byte[], byte[]> getOptimizedCache(AdvancedCache<byte[], byte[]> c, Configuration cacheCfg) {
      if (version < 20) {
         if (!hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
            switch (op) {
               case PUT:
               case PUT_IF_ABSENT:
                  return c.withFlags(Flag.IGNORE_RETURN_VALUES);
            }
         }
         return c;
      }
      boolean isTransactional = cacheCfg.transaction().transactionMode().isTransactional();
      boolean isClustered = cacheCfg.clustering().cacheMode().isClustered();

      AdvancedCache<byte[], byte[]> optCache = c;
      if (isClustered && !isTransactional && op.isConditional()) {
         log.warnConditionalOperationNonTransactional(op.toString());
      }

      if (op.canSkipCacheLoading() && hasFlag(ProtocolFlag.SkipCacheLoader)) {
         optCache = c.withFlags(Flag.SKIP_CACHE_LOAD);
      }

      if (op.canSkipIndexing() && hasFlag(ProtocolFlag.SkipIndexing)) {
         optCache = c.withFlags(Flag.SKIP_INDEXING);
      }
      if (!hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
         if (op.isNotConditionalAndCanReturnPrevious()) {
            optCache = optCache.withFlags(Flag.IGNORE_RETURN_VALUES);
         }
      } else if (!isTransactional && op.canReturnPreviousValue()) {
         log.warnForceReturnPreviousNonTransactional(op.toString());
      }
      return optCache;
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
