package org.infinispan.search.mapper.scope;

import java.util.function.Function;

import org.hibernate.search.engine.common.EntityReference;
import org.hibernate.search.engine.search.aggregation.AggregationKey;
import org.hibernate.search.engine.search.aggregation.SearchAggregation;
import org.hibernate.search.engine.search.aggregation.dsl.SearchAggregationFactory;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;
import org.hibernate.search.engine.search.projection.dsl.SearchProjectionFactory;
import org.hibernate.search.engine.search.query.dsl.SearchQueryOptionsStep;
import org.hibernate.search.engine.search.query.dsl.SearchQuerySelectStep;
import org.hibernate.search.engine.search.query.dsl.SearchQueryWhereStep;
import org.hibernate.search.engine.search.sort.dsl.SearchSortFactory;
import org.infinispan.search.mapper.session.SearchSession;

/**
 * Represents a set of types and the corresponding indexes, allowing to build search-related objects (query, predicate,
 * ...) taking into account the relevant indexes and their metadata (underlying technology, field types, ...).
 *
 * @param <E> A supertype of all types in this scope.
 */
public interface SearchScope<E> {

   /**
    * Initiate the building of a search predicate.
    * <p>
    * The predicate will only be valid for {@link SearchSession#search(SearchScope) search queries} created using this
    * scope or another scope instance targeting the same indexes.
    * <p>
    * Note this method is only necessary if you do not want to use lambda expressions, since you can {@link
    * SearchQueryWhereStep#where(Function) define predicates with lambdas} within the search query DSL, removing the
    * need to create separate objects to represent the predicates.
    *
    * @return A predicate factory.
    * @see SearchPredicateFactory
    */
   SearchPredicateFactory predicate();

   /**
    * Initiate the building of a search sort.
    * <p>
    * The sort will only be valid for {@link SearchSession#search(SearchScope) search queries} created using this scope
    * or another scope instance targeting the same indexes.
    * <p>
    * Note this method is only necessary if you do not want to use lambda expressions, since you can {@link
    * SearchQueryOptionsStep#sort(Function) define sorts with lambdas} within the search query DSL, removing the need to
    * create separate objects to represent the sorts.
    *
    * @return A sort factory.
    * @see SearchSortFactory
    */
   SearchSortFactory sort();

   /**
    * Initiate the building of a search projection that will be valid for the indexes in this scope.
    * <p>
    * The projection will only be valid for {@link SearchSession#search(SearchScope) search queries} created using this
    * scope or another scope instance targeting the same indexes.
    * <p>
    * Note this method is only necessary if you do not want to use lambda expressions, since you can {@link
    * SearchQuerySelectStep#select(Function)} define projections with lambdas} within the search query DSL,
    * removing the need to create separate objects to represent the projections.
    *
    * @return A projection factory.
    * @see SearchProjectionFactory
    */
   SearchProjectionFactory<EntityReference, E> projection();

   /**
    * Initiate the building of a search aggregation that will be valid for the indexes in this scope.
    * <p>
    * The aggregation will only be usable in {@link SearchSession#search(SearchScope) search queries} created using this
    * scope or another scope instance targeting the same indexes.
    * <p>
    * Note this method is only necessary if you do not want to use lambda expressions, since you can {@link
    * SearchQueryOptionsStep#aggregation(AggregationKey, SearchAggregation)} define aggregations with lambdas} within
    * the search query DSL, removing the need to create separate objects to represent the aggregation.
    *
    * @return An aggregation factory.
    * @see SearchAggregationFactory
    */
   SearchAggregationFactory aggregation();

   /**
    * Create a {@link SearchWorkspace} for the indexes mapped to types in this scope, or to any of their sub-types.
    *
    * @return A {@link SearchWorkspace}.
    */
   SearchWorkspace workspace();

}
