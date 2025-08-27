package org.infinispan.query.remote.client.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.jboss.marshalling.Externalize;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Externalize(Externalizers.QueryResponseExternalizer.class)
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_QUERY_RESPONSE)
public final class QueryResponse implements BaseQueryResponse {

   private int numResults;

   private int projectionSize;

   private List<WrappedMessage> results;

   private int hitCount;

   private boolean hitCountExact;

   @ProtoField(1)
   public int getNumResults() {
      return numResults;
   }

   public void setNumResults(int numResults) {
      this.numResults = numResults;
   }

   @ProtoField(2)
   public int getProjectionSize() {
      return projectionSize;
   }

   public void setProjectionSize(int projectionSize) {
      this.projectionSize = projectionSize;
   }

   @ProtoField(number = 3, collectionImplementation = ArrayList.class)
   public List<WrappedMessage> getResults() {
      return results;
   }

   public void setResults(List<WrappedMessage> results) {
      this.results = results;
   }

   @Override
   public List<?> extractResults(SerializationContext serializationContext) throws IOException {
      List<Object> unwrappedResults;
      if (projectionSize > 0) {
         unwrappedResults = new ArrayList<>(results.size() / projectionSize);
         Iterator<WrappedMessage> it = results.iterator();
         while (it.hasNext()) {
            Object[] row = new Object[projectionSize];
            for (int i = 0; i < row.length; i++) {
               Object value = it.next().getValue();
               if (value instanceof WrappedMessage) {
                  Object content = ((WrappedMessage) value).getValue();
                  if (content instanceof byte[]) {
                     value = ProtobufUtil.fromWrappedByteArray(serializationContext, (byte[]) content);
                  }
               }
               row[i] = value;
            }
            unwrappedResults.add(row);
         }
      } else {
         unwrappedResults = new ArrayList<>(results.size());
         for (WrappedMessage r : results) {
            Object o = r.getValue();
            if (serializationContext != null && o instanceof byte[]) {
               o = ProtobufUtil.fromWrappedByteArray(serializationContext, (byte[]) o);
            }
            unwrappedResults.add(o);
         }
      }
      return unwrappedResults;
   }

   @Override
   @ProtoField(4)
   public int hitCount() {
      return hitCount;
   }

   public void hitCount(int hitCount) {
      this.hitCount = hitCount;
   }

   @Override
   @ProtoField(number = 5, defaultValue = "false")
   public boolean hitCountExact() {
      return hitCountExact;
   }

   public void hitCountExact(boolean hitCountExact) {
      this.hitCountExact = hitCountExact;
   }
}
