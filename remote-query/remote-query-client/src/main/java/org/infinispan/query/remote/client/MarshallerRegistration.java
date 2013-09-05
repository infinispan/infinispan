package org.infinispan.query.remote.client;

import com.google.protobuf.Descriptors;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class MarshallerRegistration {

   public static void registerMarshallers(SerializationContext ctx) throws IOException, Descriptors.DescriptorValidationException {
      ctx.registerProtofile(MarshallerRegistration.class.getResourceAsStream("/query.protobin"));
      ctx.registerMarshaller(QueryRequest.class, new QueryRequestMarshaller());
      ctx.registerMarshaller(QueryRequest.SortCriteria.class, new SortCriteriaMarshaller());
      ctx.registerMarshaller(QueryResponse.class, new QueryResponseMarshaller());
   }
}