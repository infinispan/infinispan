package org.infinispan.server.resp.commands.connection;

import org.ehcache.sizeof.SizeOf;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;

public class MemoryEntrySizeUtils {
   private static CacheEntrySizeCalculator<byte[], Object> cesc = new CacheEntrySizeCalculator<byte[], Object>(
         MemoryEntrySizeUtils::internalCalculateSize);
   private static SizeOf sizeof = SizeOf.newInstance();

   public static long calculateSize(byte[] key, InternalCacheEntry<byte[], Object> ice) {
      return cesc.calculateSize(key, ice);
   }

   static long internalCalculateSize(byte[] key, Object value) {
      return sizeof.deepSizeOf(key) + sizeof.deepSizeOf(value);
   }
}
