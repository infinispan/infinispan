package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(TopDocs.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_TOP_DOCS)
public class LuceneTopDocsAdapter {
   @ProtoFactory
   static TopDocs protoFactory(long totalHits, ScoreDoc[] scoreDocs) {
      return new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);
   }

   @ProtoField(value = 1, defaultValue = "0")
   long getTotalHits(TopDocs topDocs) {
      return topDocs.totalHits.value;
   }

   @ProtoField(2)
   ScoreDoc[] getScoreDocs(TopDocs topDocs) {
      return topDocs.scoreDocs;
   }
}
