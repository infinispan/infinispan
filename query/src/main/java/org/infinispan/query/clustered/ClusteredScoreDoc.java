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
public final class ClusteredScoreDoc implements ClusteredDoc {

   private final UUID nodeUuid;
   private final int index;
   private final ScoreDoc scoreDoc;

   public ClusteredScoreDoc(ScoreDoc scoreDoc, UUID nodeUuid, int index) {
      this.scoreDoc = scoreDoc;
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
