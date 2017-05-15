package org.infinispan.query.clustered;

import java.io.Serializable;
import java.util.UUID;

import org.infinispan.remoting.transport.Address;

/**
 * QueryResponse.
 *
 * A response of a request to create a new distributed lazy iterator
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class QueryResponse implements Serializable {

   private static final long serialVersionUID = -2113889511877165954L;

   private UUID nodeUUID;

   private NodeTopDocs nodeTopDocs;

   private Address address;

   private Integer resultSize;

   private Object fetchedValue;

   public NodeTopDocs getTopDocs() {
      return nodeTopDocs;
   }

   public QueryResponse(Object value) {
      fetchedValue = value;
   }

   public QueryResponse(int resultSize) {
      this.resultSize = resultSize;
   }

   public QueryResponse(NodeTopDocs nodeTopDocs, UUID nodeUUid, int resultSize) {
      this.nodeUUID = nodeUUid;
      this.nodeTopDocs = nodeTopDocs;
      this.resultSize = resultSize;
   }

   public boolean nonEmpty() {
      return resultSize > 0;
   }

   public ClusteredTopDocs toClusteredTopDocs() {
      if(nodeTopDocs == null) throw new IllegalStateException("ClusteredTopDocs can't be created from an empty QueryResponse");
      ClusteredTopDocs clusteredTopDocs = new ClusteredTopDocs(this.getTopDocs(), this.getNodeUUID());
      clusteredTopDocs.setNodeAddress(this.getAddress());
      return clusteredTopDocs;
   }

   public int getResultSize() {
      return resultSize;
   }

   public UUID getNodeUUID() {
      return nodeUUID;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   public Address getAddress() {
      return address;
   }

   public Object getFetchedValue() {
      return fetchedValue;
   }

}
