package org.infinispan.server.core.query.impl.indexing.aggregator;

import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.server.core.query.impl.indexing.IndexingMessageContext;

public interface TypeAggregator {

   void add(FieldDescriptor fieldDescriptor, Object tagValue);

   void addValue(IndexingMessageContext document);

}
