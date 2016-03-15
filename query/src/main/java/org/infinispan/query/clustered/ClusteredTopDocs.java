package org.infinispan.query.clustered;

import java.util.UUID;

import org.apache.lucene.search.ScoreDoc;
import org.infinispan.remoting.transport.Address;

/**
 * ClusteredTopDocs.
 *
 * A TopDocs with UUID and address of node who has the doc.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredTopDocs {

   private int currentIndex = 0;

   private final NodeTopDocs nodeTopDocs;

   private final UUID id;

   private Address nodeAddress;

   ClusteredTopDocs(NodeTopDocs nodeTopDocs, UUID id) {
      this.nodeTopDocs = nodeTopDocs;
      this.id = id;
   }

   public UUID getId() {
      return id;
   }

   public boolean hasNext() {
      return !(currentIndex >= nodeTopDocs.topDocs.scoreDocs.length);
   }

   public NodeTopDocs getNodeTopDocs() {
      return nodeTopDocs;
   }

   public ClusteredDoc getNext() {
      if (currentIndex >= nodeTopDocs.topDocs.scoreDocs.length)
         return null;

      ScoreDoc scoreDoc = nodeTopDocs.topDocs.scoreDocs[currentIndex];
      return new ClusteredScoreDoc(scoreDoc, id, currentIndex++);
   }

   public void setNodeAddress(Address nodeAddress) {
      this.nodeAddress = nodeAddress;
   }

   public Address getNodeAddress() {
      return nodeAddress;
   }
}
