package org.infinispan.query.dsl.embedded.impl;

import java.util.Map;

import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.query.SearchQuery;
import org.hibernate.search.engine.search.sort.SearchSort;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;

/**
 * Result of {@link SearchQueryMaker#transform(IckleParsingResult, Map, Class, String)}.
 * Unlike a {@link SearchQuery} the instances can be cached and reuse later from different threads.
 *
 * @author Fabio Massimo Ercoli
 */
public final class SearchQueryParsingResult {

   private final Class<?> targetedType;
   private final String targetedTypeName;
   private final SearchProjectionInfo projectionInfo;
   private final SearchPredicate predicate;
   private final SearchSort sort;

   public SearchQueryParsingResult(Class<?> targetedType, String targetedTypeName, SearchProjectionInfo projectionInfo,
                                   SearchPredicate predicate, SearchSort sort) {
      this.targetedType = targetedType;
      this.targetedTypeName = targetedTypeName;
      this.projectionInfo = projectionInfo;
      this.predicate = predicate;
      this.sort = sort;
   }

   public SearchQueryBuilder builder(SearchSession querySession) {
      SearchScope<?> scope = getScope(querySession);
      return new SearchQueryBuilder(querySession, scope, projectionInfo, predicate, sort);
   }

   private SearchScope<?> getScope(SearchSession querySession) {
      return targetedTypeName == null ? querySession.scope(targetedType) :
            querySession.scope(targetedType, targetedTypeName);
   }
}
