package org.infinispan.query.remote.impl.indexing.aggregator;

import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.remote.impl.indexing.IndexingMessageContext;

public interface TypeAggregator {

   void add(FieldDescriptor fieldDescriptor, Object tagValue);

   void addValue(IndexingMessageContext document);

}
