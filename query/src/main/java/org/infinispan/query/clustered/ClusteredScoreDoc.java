package org.infinispan.query.clustered;

import java.util.UUID;

import org.apache.lucene.search.ScoreDoc;

/**
 * ClusteredScoreDoc.
 * 
 * A scoreDoc with his index and the uuid of the node who has the value.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredScoreDoc extends ScoreDoc implements ClusteredDoc {

   private static final long serialVersionUID = -951189455330773036L;

   private final UUID nodeUuid;

   private final int index;

   public ClusteredScoreDoc(ScoreDoc scoreDoc, UUID nodeUuid, int index) {
      super(scoreDoc.doc, scoreDoc.score);
      this.nodeUuid = nodeUuid;
      this.index = index;
   }

   @Override
   public UUID getNodeUuid() {
      return nodeUuid;
   }

   @Override
   public int getIndex() {
      return index;
   }

}
