package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.ScoreDoc;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(ScoreDoc.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_SCORE_DOC)
public class LuceneScoreDocAdapter {

   @ProtoFactory
   static ScoreDoc protoFactory(int doc, float score, int shardIndex) {
      return new ScoreDoc(doc, score, shardIndex);
   }

   @ProtoField(1)
   int getDoc(ScoreDoc scoreDoc) {
      return scoreDoc.doc;
   }

   @ProtoField(2)
   float getScore(ScoreDoc scoreDoc) {
      return scoreDoc.score;
   }

   @ProtoField(3)
   int getShardIndex(ScoreDoc scoreDoc) {
      return scoreDoc.shardIndex;
   }
}
