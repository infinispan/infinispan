package org.infinispan.query.remote.impl;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.core.impl.EmbeddedQueryFactory;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.impl.BaseQuery;

/**
 * A QueryEngine for remote, dealing with entries stored as objects rather than marshalled.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
class ObjectRemoteQueryEngine extends QueryEngine<Descriptor> {

   private final EmbeddedQueryFactory queryFactory = new EmbeddedQueryFactory(this);

   ObjectRemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed, Class<? extends Matcher> matcherImplClass) {
      super(cache.getAdvancedCache().withEncoding(IdentityEncoder.class), isIndexed, matcherImplClass);
   }

   BaseQuery<Object> makeQuery(String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults) {
      BaseQuery<Object> query = queryFactory.create(queryString);
      query.startOffset(startOffset);
      query.maxResults(maxResults);
      if (namedParameters != null) {
         query.setParameters(namedParameters);
      }
      return query;
   }
}
