package org.infinispan.query.remote.client.impl;

import java.io.IOException;
import java.util.List;

import org.infinispan.protostream.SerializationContext;

/**
 * @since 9.4
 */
public interface BaseQueryResponse<T> {

   List<T> extractResults(SerializationContext serializationContext) throws IOException;

   int hitCount();

   boolean hitCountExact();

}
