package org.infinispan.query.remote.indexing;

import com.google.protobuf.Descriptors;
import org.infinispan.protostream.MessageContext;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class ReadMessageContext extends MessageContext<ReadMessageContext> {

   ReadMessageContext(ReadMessageContext parentContext, String fieldName, Descriptors.Descriptor messageDescriptor) {
      super(parentContext, fieldName, messageDescriptor);
   }
}
