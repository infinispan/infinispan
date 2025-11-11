package org.infinispan.query.impl.protostream.adapters;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(TotalHits.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_TOTAL_HITS)
public class LuceneTotalHitsAdapter {
   // These MethodHandles allow us to retrieve the values from TotalHits as record components (Lucene 10) or as public
   // fields (Lucene 9)
   static final MethodHandle valueHandle = ReflectionUtil.getterOf(MethodHandles.lookup(), TotalHits.class, "value", long.class);
   static final MethodHandle relationHandle = ReflectionUtil.getterOf(MethodHandles.lookup(), TotalHits.class, "relation", TotalHits.Relation.class);

   @ProtoFactory
   static TotalHits protoFactory(long value, TotalHits.Relation relation) {
      return new TotalHits(value, relation);
   }

   @ProtoField(1)
   long getValue(TotalHits totalHits) {
      try {
         return (long) valueHandle.invoke(totalHits);
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
   }

   @ProtoField(2)
   TotalHits.Relation getRelation(TotalHits totalHits) {
      try {
         return (TotalHits.Relation) relationHandle.invoke(totalHits);
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
   }
}
