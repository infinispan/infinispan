package org.infinispan.query.remote.impl;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.core.impl.EmbeddedQueryFactory;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.objectfilter.Matcher;

/**
 * A QueryEngine for remote, dealing with entries stored as objects rather than marshalled.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
class ObjectRemoteQueryEngine extends QueryEngine<Descriptor> {

   private final EmbeddedQueryFactory queryFactory = new EmbeddedQueryFactory(this);

   ObjectRemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed, Class<? extends Matcher> matcherImplClass) {
      super(cache.getAdvancedCache(), isIndexed, matcherImplClass);
   }

   Query<Object> makeQuery(String queryString, Map<String, Object> namedParameters, Number startOffset, Number maxResults,
                           Integer hitCountAccuracy, Boolean local) {
      Query<Object> query = queryFactory.create(queryString);
      if (startOffset != null) {
         query.startOffset(startOffset.intValue());
      }
      if (maxResults != null) {
         query.maxResults(maxResults.intValue());
      }
      if (hitCountAccuracy != null) {
         query.hitCountAccuracy(hitCountAccuracy);
      }
      if (local != null) {
         query.local(local);
      }
      if (namedParameters != null) {
         query.setParameters(namedParameters);
      }
      return query;
   }
}
