package org.infinispan.query.remote.impl;

import java.io.IOException;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * {@link RemoteQueryManager} suitable for caches storing protobuf.
 *
 * @since 9.2
 */
class ProtobufRemoteQueryManager implements RemoteQueryManager {

   private static final Log log = LogFactory.getLog(ProtobufRemoteQueryManager.class, Log.class);

   private final Matcher matcher;
   private final BaseRemoteQueryEngine queryEngine;
   private final SerializationContext serCtx;
   private final Encoder keyEncoder;
   private final Encoder valueEncoder;

   ProtobufRemoteQueryManager(SerializationContext serCtx, ComponentRegistry cr) {
      this.serCtx = serCtx;
      this.matcher = new ProtobufMatcher(serCtx, ProtobufFieldIndexingMetadata::new);
      cr.registerComponent(matcher, ProtobufMatcher.class);
      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      boolean isIndexed = cr.getComponent(Configuration.class).indexing().index().isEnabled();
      this.queryEngine = new RemoteQueryEngine(cache, isIndexed);
      this.keyEncoder = cache.getKeyDataConversion().getEncoder();
      this.valueEncoder = cache.getValueDataConversion().getEncoder();
   }

   @Override
   public Matcher getMatcher() {
      return matcher;
   }

   @Override
   public BaseRemoteQueryEngine getQueryEngine() {
      return queryEngine;
   }

   @Override
   public QueryRequest decodeQueryRequest(byte[] queryRequest) {
      try {
         return ProtobufUtil.fromByteArray(queryEngine.getSerializationContext(), queryRequest, 0, queryRequest.length, QueryRequest.class);
      } catch (IOException e) {
         throw log.errorExecutingQuery(e);
      }
   }

   @Override
   public byte[] encodeQueryResponse(QueryResponse queryResponse) {
      try {
         return ProtobufUtil.toByteArray(queryEngine.getSerializationContext(), queryResponse);
      } catch (IOException e) {
         throw log.errorExecutingQuery(e);
      }
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      try {
         return ProtobufUtil.toWrappedByteArray(serCtx, filterResult);
      } catch (IOException e) {
         throw log.errorFiltering(e);
      }
   }

   @Override
   public Encoder getKeyEncoder() {
      return keyEncoder;
   }

   @Override
   public Encoder getValueEncoder() {
      return valueEncoder;
   }
}
