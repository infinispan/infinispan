package org.infinispan.server.resp;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @since 15.0
 **/
@ProtoTypeId(ProtoStreamTypeIds.RESP_TYPES)
public enum RespTypes {
   @ProtoEnumValue(1)
   none,
   @ProtoEnumValue(2)
   hash,
   @ProtoEnumValue(3)
   list,
   @ProtoEnumValue(4)
   set,
   // We must modify the name as 'stream' is a reserved keyword in Protostream
   @ProtoEnumValue(value = 5, name = "_stream")
   stream,
   @ProtoEnumValue(6)
   string,
   @ProtoEnumValue(7)
   zset,

   // Not a real Resp type
   @ProtoEnumValue(0)
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
