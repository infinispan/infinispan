package org.infinispan.query.remote.client;

import java.io.IOException;
import java.util.List;

import org.infinispan.protostream.SerializationContext;

/**
 * @since 9.4
 */
public interface BaseQueryResponse {

   List<?> extractResults(SerializationContext serializationContext) throws IOException;

   long getTotalResults();

}
