package org.infinispan.server.hotrod;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;

/**
 * Utility class in hotrod server with methods handling cache entries metadata
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public final class MetadataUtils {

   private MetadataUtils() {
   }

   public static void writeMetadata(int lifespan, int maxIdle, long created, long lastUsed, long dataVersion, ByteBuf buf) {
      int flags = (lifespan < 0 ? Constants.INFINITE_LIFESPAN : 0) + (maxIdle < 0 ? Constants.INFINITE_MAXIDLE : 0);
      buf.writeByte(flags);
      if (lifespan >= 0) {
         buf.writeLong(created);
         ExtendedByteBuf.writeUnsignedInt(lifespan, buf);
      }
      if (maxIdle >= 0) {
         buf.writeLong(lastUsed);
         ExtendedByteBuf.writeUnsignedInt(maxIdle, buf);
      }
      buf.writeLong(dataVersion);
   }

   public static long extractVersion(CacheEntry ce) {
      if (ce == null) return -1;

      EntryVersion entryVersion = ce.getMetadata().version();
      long version = 0;
      if (entryVersion != null) {
         if (entryVersion instanceof NumericVersion) {
            version = NumericVersion.class.cast(entryVersion).getVersion();
         }
         if (entryVersion instanceof SimpleClusteredVersion) {
            version = SimpleClusteredVersion.class.cast(entryVersion).getVersion();
         }
      }
      return version;
   }

   public static long extractCreated(CacheEntry ce) {
      if (ce == null) return -1;

      return ce.getCreated();
   }

   public static long extractLastUsed(CacheEntry ce) {
      if (ce == null) return -1;

      return ce.getLastUsed();
   }

   public static int extractLifespan(CacheEntry ce) {
      if (ce == null) return -1;

      return ce.getLifespan() < 0 ? -1 : (int) (ce.getLifespan() / 1000);
   }

   public static int extractMaxIdle(CacheEntry ce) {
      if (ce == null) return -1;

      return ce.getMaxIdle() < 0 ? -1 : (int) (ce.getMaxIdle() / 1000);
   }
}
