package org.infinispan.server.resp;

import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.multimap.impl.SortedSetBucket;

/**
 * @since 15.0
 **/
public enum RespTypes {
   none,
   hash,
   list,
   set,
   stream,
   string,
   zset,
   // Not a real Resp type
   unknown;

   public static RespTypes fromOrdinal(byte ordinal) {
      return values()[ordinal];
   }

   public static RespTypes fromValueClass(Class<?> c) {
      if (c == HashMapBucket.class) {
         return RespTypes.hash;
      } else if (c == ListBucket.class) {
         return RespTypes.list;
      } else if (c == SetBucket.class) {
         return RespTypes.set;
      } else if (c == SortedSetBucket.class) {
         return RespTypes.zset;
      } else if (c == byte[].class) {
         return RespTypes.string;
      } else {
         return RespTypes.unknown;
      }
   }
}
