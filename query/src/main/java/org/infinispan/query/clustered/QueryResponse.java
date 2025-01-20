package org.infinispan.query.clustered;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The response for a {@link ClusteredQueryOperation}.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
@ProtoTypeId(ProtoStreamTypeIds.QUERY_RESPONSE)
public final class QueryResponse {

   private final NodeTopDocs nodeTopDocs;
   private final int resultSize;
   private final boolean countIsExact;

   public QueryResponse(int resultSize) {
      this.resultSize = resultSize;
      countIsExact = true; // count from CQGetResultSize is always exact
      nodeTopDocs = null;
   }

   public QueryResponse(NodeTopDocs nodeTopDocs) {
      this.resultSize = nodeTopDocs.totalHitCount;
      this.countIsExact = nodeTopDocs.countIsExact;
      this.nodeTopDocs = nodeTopDocs;
   }

   @ProtoFactory
   static QueryResponse protoFactory(NodeTopDocs nodeTopDocs, Integer boxedResultSize) {
      return nodeTopDocs == null ? new QueryResponse(boxedResultSize) : new QueryResponse(nodeTopDocs);
   }

   @ProtoField(1)
   public NodeTopDocs getNodeTopDocs() {
      return nodeTopDocs;
   }

   @ProtoField(value = 2, name = "resultSize")
   Integer getBoxedResultSize() {
      return nodeTopDocs == null ? resultSize : null;
   }

   public int getResultSize() {
      return resultSize;
   }

   public boolean countIsExact() {
      return countIsExact;
   }
}
