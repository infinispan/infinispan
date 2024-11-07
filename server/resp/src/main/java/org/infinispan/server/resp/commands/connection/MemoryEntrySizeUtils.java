package org.infinispan.server.resp.commands.connection;

import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.multimap.impl.SortedSetBucket;

public class MemoryEntrySizeUtils {
   private static PrimitiveEntrySizeCalculator pesc = new PrimitiveEntrySizeCalculator();
   private static  CacheEntrySizeCalculator<byte[], Object> cesc = new CacheEntrySizeCalculator<byte[], Object>(MemoryEntrySizeUtils::internalCalculateSize);
   public static long calculateSize(byte[] key, InternalCacheEntry<byte[], Object> ice) {
      return cesc.calculateSize(key, ice);
   }

   private static long internalCalculateSize(byte[] key, Object value) {
         try {
            return pesc.calculateSize(key, value);
         } catch (IllegalArgumentException e) {
            // Go ahead trying with RESP types
         }
      if ( value.getClass() == SetBucket.class ) {
         return 1L; //todo: fix this with correct computation
      } else if ( value.getClass() == HashMapBucket.class) {
         return 1L; //todo: fix this with correct computation
      } else if ( value.getClass() == ListBucket.class) {
         return 1L; //todo: fix this with correct computation
      } else if ( value.getClass() == SortedSetBucket.class) {
         return 1L; //todo: fix this with correct computation
      }
      throw new IllegalArgumentException("Size of Class " + value.getClass() +
      " cannot be determined using given entry size calculator :" + MemoryEntrySizeUtils.class.getName());
   }
}
