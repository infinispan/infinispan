package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.FieldDoc;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(FieldDoc.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_FIELD_DOC)
public class LuceneFieldDocAdapter {
   @ProtoFactory
   static FieldDoc protoFactory(int doc, float score, MarshallableArray<Object> fields, int shardIndex) {
      return new FieldDoc(doc, score, MarshallableArray.unwrap(fields, new Object[0]), shardIndex);
   }

   @ProtoField(value = 1, defaultValue = "0")
   int getDoc(FieldDoc fieldDoc) {
      return fieldDoc.doc;
   }

   @ProtoField(value = 2, defaultValue = "0.0")
   float getScore(FieldDoc fieldDoc) {
      return fieldDoc.score;
   }

   @ProtoField(3)
   MarshallableArray<Object> getFields(FieldDoc fieldDoc) {
      return MarshallableArray.create(fieldDoc.fields);
   }

   @ProtoField(value = 4, defaultValue = "0")
   int getShardIndex(FieldDoc fieldDoc) {
      return fieldDoc.shardIndex;
   }
}
