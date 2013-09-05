package org.infinispan.query.remote.client;

import org.infinispan.protostream.WrappedMessage;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class QueryResponse {

   private int numResults;

   private int projectionSize;

   private List<WrappedMessage> results;

   public int getNumResults() {
      return numResults;
   }

   public void setNumResults(int numResults) {
      this.numResults = numResults;
   }

   public int getProjectionSize() {
      return projectionSize;
   }

   public void setProjectionSize(int projectionSize) {
      this.projectionSize = projectionSize;
   }

   public List<WrappedMessage> getResults() {
      return results;
   }

   public void setResults(List<WrappedMessage> results) {
      this.results = results;
   }
}
