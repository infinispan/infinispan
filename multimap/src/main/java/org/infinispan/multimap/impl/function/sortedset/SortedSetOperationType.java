package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

@Proto
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_OPERATION_TYPE)
public enum SortedSetOperationType {
   INDEX, SCORE, LEX, OTHER;
   private static final SortedSetOperationType[] CACHED_VALUES = values();
   public static SortedSetOperationType valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }
}
