package org.infinispan.server.hotrod.test;

import org.infinispan.commons.util.Util;

/**
 * @author wburns
 * @since 9.0
 */
public class Op {
   final int magic;
   final byte version;
   final byte code;
   final String cacheName;
   final byte[] key;
   final int lifespan;
   final int maxIdle;
   final byte[] value;
   final int flags;
   final long dataVersion;
   final byte clientIntel;
   final int topologyId;
   final long id = HotRodClient.idCounter.incrementAndGet();

   public Op(int magic, byte version, byte code, String cacheName, byte[] key, int lifespan, int maxIdle, byte[] value,
             int flags, long dataVersion, byte clientIntel, int topologyId) {
      this.magic = magic;
      this.version = version;
      this.code = code;
      this.cacheName = cacheName;
      this.key = key;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.value = value;
      this.flags = flags;
      this.dataVersion = dataVersion;
      this.clientIntel = clientIntel;
      this.topologyId = topologyId;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("Op{");
      sb.append("magic=").append(magic);
      sb.append(", version=").append(version);
      sb.append(", code=").append(code);
      sb.append(", cacheName='").append(cacheName).append('\'');
      sb.append(", key=");
      sb.append(Util.printArray(key));
      sb.append(", lifespan=").append(lifespan);
      sb.append(", maxIdle=").append(maxIdle);
      sb.append(", value=");
      sb.append(Util.printArray(value));
      sb.append(", flags=").append(flags);
      sb.append(", dataVersion=").append(dataVersion);
      sb.append(", clientIntel=").append(clientIntel);
      sb.append(", topologyId=").append(topologyId);
      sb.append(", id=").append(id);
      sb.append('}');
      return sb.toString();
   }
}
