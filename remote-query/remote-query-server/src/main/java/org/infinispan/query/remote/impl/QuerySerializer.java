package org.infinispan.query.remote.impl;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.query.remote.client.QueryRequest;

/**
 * Handles serialization of query results.
 *
 * @since 9.4
 */
interface QuerySerializer<QR> {

   QueryRequest decodeQueryRequest(byte[] queryRequest, MediaType mediaType);

   QR createQueryResponse(RemoteQueryResult remoteQueryResult);

   byte[] encodeQueryResponse(Object queryResponse, MediaType requestType);

}
