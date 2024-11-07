package org.infinispan.server.resp.commands.connection;

import org.ehcache.sizeof.SizeOf;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;

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
         return sizeof.deepSizeOf(key) + sizeof.deepSizeOf(value);
      } catch (Exception ex) {
         // Try an old style computation
         return pesc.calculateSize(key, value);
      }
   }
}
