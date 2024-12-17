package org.infinispan.query.impl.protostream.adapters;

import java.util.stream.Stream;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(TopFieldDocs.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_TOP_FIELD_DOCS)
public class LuceneTopFieldDocsAdapter {

   @ProtoFactory
   static TopFieldDocs protoFactory(TotalHits totalHits, WrappedMessage[] scoreDocs, SortField[] sortFields) {
      ScoreDoc[] docs = Stream.of(scoreDocs).map(WrappedMessages::unwrap).toArray(ScoreDoc[]::new);
      return new TopFieldDocs(totalHits, docs, sortFields);
   }

   @ProtoField(1)
   TotalHits getTotalHits(TopFieldDocs topFieldDocs) {
      return topFieldDocs.totalHits;
   }

   @ProtoField(2)
   WrappedMessage[] getScoreDocs(TopFieldDocs topFieldDocs) {
      // We must use a WrappedMessage here to allow for inheritance as this can either be FieldDoc or ScoreDoc
      return Stream.of(topFieldDocs.scoreDocs).map(WrappedMessages::orElseNull).toArray(WrappedMessage[]::new);
   }

   @ProtoField(3)
   SortField[] getSortFields(TopFieldDocs topFieldDocs) {
      return topFieldDocs.fields;
   }
}
