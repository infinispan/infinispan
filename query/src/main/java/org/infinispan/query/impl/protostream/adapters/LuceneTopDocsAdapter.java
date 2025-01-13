package org.infinispan.query.impl.protostream.adapters;

import java.util.stream.Stream;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(TopDocs.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_TOP_DOCS)
public class LuceneTopDocsAdapter {
   @ProtoFactory
   static TopDocs protoFactory(long totalHits, WrappedMessage[] scoreDocs) {
      ScoreDoc[] docs = Stream.of(scoreDocs).map(WrappedMessages::unwrap).toArray(ScoreDoc[]::new);
      return new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), docs);
   }

   @ProtoField(value = 1, defaultValue = "0")
   long getTotalHits(TopDocs topDocs) {
      return topDocs.totalHits.value;
   }

   @ProtoField(2)
   WrappedMessage[] getScoreDocs(TopDocs topDocs) {
      // We must use a WrappedMessage here to allow for inheritance as this can either be FieldDoc or ScoreDoc
      return Stream.of(topDocs.scoreDocs).map(WrappedMessages::orElseNull).toArray(WrappedMessage[]::new);
   }
}
