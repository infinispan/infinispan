package org.infinispan.query.remote.impl.indexing.aggregator;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.query.remote.impl.indexing.IndexingMessageContext;

public class BigDecimalAggregator implements TypeAggregator {

   private final IndexFieldReference<?> fieldReference;

   private byte[] unscaledValue;

   private int scale;

   public BigDecimalAggregator(IndexFieldReference<?> fieldReference) {
      this.fieldReference = fieldReference;
   }

   @Override
   public void add(FieldDescriptor fieldDescriptor, Object tagValue) {
      if (Type.BYTES.equals(fieldDescriptor.getType())) {
         unscaledValue = (byte[]) tagValue;
      } else if (Type.INT32.equals(fieldDescriptor.getType())) {
         scale = (int) tagValue;
      }
   }

   @Override
   public void addValue(IndexingMessageContext document) {
      if (unscaledValue != null) {
         document.addValue(fieldReference, new BigDecimal(new BigInteger(unscaledValue), scale));
      }
   }
}
