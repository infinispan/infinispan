package org.infinispan.query.clustered;

import java.util.UUID;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
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

   private final TopDocs topDocs;

   private final UUID id;

   private Address nodeAddress;

   ClusteredTopDocs(TopDocs topDocs, UUID id) {
      this.topDocs = topDocs;
      this.id = id;
   }

   public UUID getId() {
      return id;
   }

   public boolean hasNext() {
      return !(currentIndex >= topDocs.scoreDocs.length);
   }

   public TopDocs getTopDocs() {
      return topDocs;
   }

   public ClusteredDoc getNext() {
      if (currentIndex >= topDocs.scoreDocs.length)
         return null;

      ScoreDoc scoreDoc = topDocs.scoreDocs[currentIndex];
      return new ClusteredScoreDoc(scoreDoc, id, currentIndex++);
   }

   public void setNodeAddress(Address nodeAddress) {
      this.nodeAddress = nodeAddress;
   }

   public Address getNodeAddress() {
      return nodeAddress;
   }
}