package org.infinispan.query.clustered;

import java.io.Serializable;
import java.util.UUID;

import org.apache.lucene.search.TopDocs;
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

   private TopDocs topDocs;

   private Address address;

   private Integer resultSize;

   private Object fetchedValue;

   public TopDocs getTopDocs() {
      return topDocs;
   }

   public QueryResponse(Object value) {
      fetchedValue = value;
   }

   public QueryResponse(int resultSize) {
      this.resultSize = resultSize;
   }

   public QueryResponse(TopDocs topDocs, UUID nodeUUid, int resultSize) {
      this.nodeUUID = nodeUUid;
      this.topDocs = topDocs;
      this.resultSize = resultSize;
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