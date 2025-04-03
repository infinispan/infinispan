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

   public static long calculateSize(Object o) {
      return sizeof.deepSizeOf(o);
   }

   static long internalCalculateSize(byte[] key, Object value) {
      try {
         if (value instanceof JsonBucket jb) {
            // JsonBucket is a special case, we need to use the sizeOf library
            // to calculate the size of the JsonBucket
            return sizeof.deepSizeOf(key) + sizeof.deepSizeOf(jb.value())+ JsonBucket.memoryHeaderSize();
         }
         return sizeof.deepSizeOf(key) + sizeof.deepSizeOf(value);
      } catch (Exception ex) {
         // Try an old style computation
         return pesc.calculateSize(key, value);
      }
   }
}
