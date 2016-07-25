package org.infinispan.server.hotrod.test;

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
      final StringBuffer sb = new StringBuffer("Op{");
      sb.append("magic=").append(magic);
      sb.append(", version=").append(version);
      sb.append(", code=").append(code);
      sb.append(", cacheName='").append(cacheName).append('\'');
      sb.append(", key=");
      if (key == null) sb.append("null");
      else {
         sb.append('[');
         for (int i = 0; i < key.length; ++i)
            sb.append(i == 0 ? "" : ", ").append(key[i]);
         sb.append(']');
      }
      sb.append(", lifespan=").append(lifespan);
      sb.append(", maxIdle=").append(maxIdle);
      sb.append(", value=");
      if (value == null) sb.append("null");
      else {
         sb.append('[');
         for (int i = 0; i < value.length; ++i)
            sb.append(i == 0 ? "" : ", ").append(value[i]);
         sb.append(']');
      }
      sb.append(", flags=").append(flags);
      sb.append(", dataVersion=").append(dataVersion);
      sb.append(", clientIntel=").append(clientIntel);
      sb.append(", topologyId=").append(topologyId);
      sb.append(", id=").append(id);
      sb.append('}');
      return sb.toString();
   }
}
