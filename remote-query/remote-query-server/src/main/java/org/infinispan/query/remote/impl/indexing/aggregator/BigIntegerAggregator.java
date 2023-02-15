package org.infinispan.query.remote.impl.indexing.aggregator;

import java.math.BigInteger;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.query.remote.impl.indexing.IndexingMessageContext;

public class BigIntegerAggregator implements TypeAggregator {

   private final IndexFieldReference<?> fieldReference;

   private byte[] bytes;

   public BigIntegerAggregator(IndexFieldReference<?> fieldReference) {
      this.fieldReference = fieldReference;
   }

   @Override
   public void add(FieldDescriptor fieldDescriptor, Object tagValue) {
      if (Type.BYTES.equals(fieldDescriptor.getType())) {
         bytes = (byte[]) tagValue;
      }
   }

   @Override
   public void addValue(IndexingMessageContext document) {
      if (bytes != null) {
         document.addValue(fieldReference, new BigInteger(bytes));
      }
   }
}
