package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(TopFieldDocs.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_TOP_FIELD_DOCS)
public class LuceneTopFieldDocsAdapter {

   @ProtoFactory
   static TopFieldDocs protoFactory(TotalHits totalHits, MarshallableArray<ScoreDoc> scoreDocs, SortField[] sortFields) {
      return new TopFieldDocs(totalHits, MarshallableArray.unwrap(scoreDocs, new ScoreDoc[0]), sortFields);
   }

   @ProtoField(1)
   TotalHits getTotalHits(TopFieldDocs topFieldDocs) {
      return topFieldDocs.totalHits;
   }

   @ProtoField(2)
   MarshallableArray<ScoreDoc> getScoreDocs(TopFieldDocs topFieldDocs) {
      // We must use a MarshallableArray here to allow for inheritance as this can either be FieldDoc or ScoreDoc
      return MarshallableArray.create(topFieldDocs.scoreDocs);
   }

   @ProtoField(3)
   SortField[] getSortFields(TopFieldDocs topFieldDocs) {
      return topFieldDocs.fields;
   }
}
