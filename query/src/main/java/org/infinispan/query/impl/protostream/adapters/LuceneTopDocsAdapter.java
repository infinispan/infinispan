package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(TopDocs.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_TOP_DOCS)
public class LuceneTopDocsAdapter {
   @ProtoFactory
   static TopDocs protoFactory(long totalHits, MarshallableArray<ScoreDoc> scoreDocs) {
      ScoreDoc[] docs = MarshallableArray.unwrap(scoreDocs, new ScoreDoc[0]);
      return new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), docs);
   }

   @ProtoField(1)
   long getTotalHits(TopDocs topDocs) {
      return topDocs.totalHits.value;
   }

   @ProtoField(2)
   MarshallableArray<ScoreDoc> getScoreDocs(TopDocs topDocs) {
      // We must use a MarshallableArray here to allow for inheritance as this can either be FieldDoc or ScoreDoc
      return MarshallableArray.create(topDocs.scoreDocs);
   }
}
