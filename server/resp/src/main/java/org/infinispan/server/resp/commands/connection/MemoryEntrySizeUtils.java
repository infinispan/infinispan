package org.infinispan.server.resp.commands.connection;

import org.ehcache.sizeof.SizeOf;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.server.resp.json.JsonBucket;

public class MemoryEntrySizeUtils {
   private static PrimitiveEntrySizeCalculator pesc = new PrimitiveEntrySizeCalculator();
   private static CacheEntrySizeCalculator<byte[], Object> cesc = new CacheEntrySizeCalculator<byte[], Object>(
         MemoryEntrySizeUtils::internalCalculateSize);
   private static SizeOf sizeof = SizeOf.newInstance();

   public static long calculateSize(byte[] key, InternalCacheEntry<byte[], Object> ice) {
      return cesc.calculateSize(key, ice);
   }

   static long internalCalculateSize(byte[] key, Object value) {
      try {
         long keySize = sizeof.deepSizeOf(key);
         long valueSize;
         if (value instanceof JsonBucket jb) {
            // jol fails with jsonBucket, but we store it as byte[] in the cache
            valueSize = sizeof.deepSizeOf(jb.value());
         } else {
            valueSize = sizeof.deepSizeOf(value);
         }
         return keySize + valueSize;
      } catch (Exception ex) {
         // Try an old style computation
         return pesc.calculateSize(key, value);
      }
   }
}
