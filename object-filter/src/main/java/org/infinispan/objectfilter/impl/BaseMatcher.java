package org.infinispan.objectfilter.impl;

import org.hibernate.hql.QueryParser;
import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.Predicate;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.be.BENode;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
import org.infinispan.objectfilter.query.FilterQuery;
import org.infinispan.objectfilter.query.FilterQueryFactory;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
abstract class BaseMatcher<TypeMetadata, AttributeId extends Comparable<AttributeId>> implements Matcher {

   private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

   private final Lock read = readWriteLock.readLock();

   private final Lock write = readWriteLock.writeLock();

   private final QueryParser queryParser = new QueryParser();

   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   private final Map<String, FilterRegistry<AttributeId>> filtersByType = new HashMap<String, FilterRegistry<AttributeId>>();

   @Override
   public QueryFactory<Query> getQueryFactory() {
      return new FilterQueryFactory();
   }

   /**
    * Executes the registered filters and notifies each one of them whether it was satisfied or not by the given
    * instance.
    *
    * @param instance the instance to match filters against
    */
   @Override
   public void match(Object instance) {
      if (instance == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }

      read.lock();
      try {
         MatcherEvalContext<AttributeId> ctx = startContext(instance, filtersByType.keySet());
         if (ctx != null) {
            FilterRegistry<AttributeId> filterRegistry = filtersByType.get(ctx.getEntityTypeName());
            filterRegistry.match(ctx);
         }
      } finally {
         read.unlock();
      }
   }

   @Override
   public ObjectFilter getObjectFilter(Query query) {
      if (!(query instanceof FilterQuery)) {
         throw new IllegalArgumentException("The Query object must be created by a QueryFactory returned by this Matcher");
      }
      FilterQuery filterQuery = (FilterQuery) query;
      return getObjectFilter(filterQuery.getJpqlString());
   }

   @Override
   public ObjectFilter getObjectFilter(String jpaQuery) {
      FilterParsingResult<TypeMetadata> parsingResult = parse(jpaQuery);
      BooleanExpr normalizedFilter = booleanFilterNormalizer.normalize(parsingResult.getQuery());

      //todo [anistor] we need an efficient single-filter registry ...
      FilterRegistry filterRegistry = createFilterRegistryForType(parsingResult.getTargetEntityMetadata());
      FilterSubscriptionImpl filterSubscriptionImpl = filterRegistry.addFilter(normalizedFilter, parsingResult.getProjections(), new FilterCallback() {
         @Override
         public void onFilterResult(Object instance, Object[] projection) {
            // do nothing
         }
      });

      return getObjectFilter(filterSubscriptionImpl);
   }

   @Override
   public ObjectFilter getObjectFilter(FilterSubscription filterSubscription) {
      final FilterSubscriptionImpl<AttributeId> filterSubscriptionImpl = (FilterSubscriptionImpl<AttributeId>) filterSubscription;
      final Set<String> knownTypes = Collections.singleton(filterSubscriptionImpl.getEntityTypeName());
      final PredicateIndex<AttributeId> predicateIndex = new PredicateIndex<AttributeId>();

      filterSubscriptionImpl.registerProjection(predicateIndex);

      for (BENode node : filterSubscriptionImpl.getBETree().getNodes()) {
         if (node instanceof PredicateNode) {
            final PredicateNode<AttributeId> predicateNode = (PredicateNode<AttributeId>) node;
            Predicate.Callback predicateCallback = new Predicate.Callback() {
               @Override
               public void handleValue(MatcherEvalContext<?> ctx, boolean isMatching) {
                  FilterEvalContext filterEvalContext = ctx.getFilterEvalContext(filterSubscriptionImpl);
                  predicateNode.handleChildValue(null, isMatching, filterEvalContext);
               }
            };
            predicateIndex.addSubscriptionForPredicate(predicateNode, predicateCallback);
         }
      }

      return new ObjectFilter() {
         @Override
         public String[] getProjection() {
            return filterSubscriptionImpl.getProjection();
         }

         @Override
         public Object filter(Object instance) {
            if (instance == null) {
               throw new IllegalArgumentException("argument cannot be null");
            }

            MatcherEvalContext<AttributeId> matcherEvalContext = startContext(instance, knownTypes);
            if (matcherEvalContext != null) {
               FilterEvalContext filterEvalContext = matcherEvalContext.getFilterEvalContext(filterSubscriptionImpl);
               matcherEvalContext.process(predicateIndex.getRoot());

               if (filterEvalContext.getMatchResult()) {
                  Object[] projection = filterEvalContext.getProjection();
                  return projection != null ? projection : instance;
               }
            }

            return null;
         }
      };
   }

   @Override
   public FilterSubscription registerFilter(Query query, FilterCallback callback) {
      if (!(query instanceof FilterQuery)) {
         throw new IllegalArgumentException("The Query object must be created by a QueryFactory returned by this Matcher");
      }
      FilterQuery filterQuery = (FilterQuery) query;
      return registerFilter(filterQuery.getJpqlString(), callback);
   }

   @Override
   public FilterSubscription registerFilter(String jpaQuery, FilterCallback callback) {
      FilterParsingResult<TypeMetadata> parsingResult = parse(jpaQuery);
      BooleanExpr normalizedFilter = booleanFilterNormalizer.normalize(parsingResult.getQuery());

      write.lock();
      try {
         FilterRegistry<AttributeId> filterRegistry = filtersByType.get(parsingResult.getTargetEntityName());
         if (filterRegistry == null) {
            filterRegistry = createFilterRegistryForType(parsingResult.getTargetEntityMetadata());
            filtersByType.put(parsingResult.getTargetEntityName(), filterRegistry);
         }
         return filterRegistry.addFilter(normalizedFilter, parsingResult.getProjections(), callback);
      } finally {
         write.unlock();
      }
   }

   private FilterParsingResult<TypeMetadata> parse(String jpaQuery) {
      //todo [anistor] query params not yet fully supported by HQL parser. to be added later.
      return queryParser.parseQuery(jpaQuery, createFilterProcessingChain(null));
   }

   @Override
   public void unregisterFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl filterSubscriptionImpl = (FilterSubscriptionImpl) filterSubscription;
      write.lock();
      try {
         FilterRegistry<AttributeId> filterRegistry = filtersByType.get(filterSubscriptionImpl.getEntityTypeName());
         if (filterRegistry != null) {
            filterRegistry.removeFilter(filterSubscription);
         } else {
            throw new IllegalStateException("Reached illegal state");
         }
         if (filterRegistry.isEmpty()) {
            filtersByType.remove(filterRegistry.getTypeName());
         }
      } finally {
         write.unlock();
      }
   }

   /**
    * Creates a new MatcherEvalContext only if the given instance has filters registered. This method is called while
    * holding the write lock.
    *
    * @param instance   the instance to filter; never null
    * @param knownTypes
    * @return the context or null if no filter was registered for the instance
    */
   protected abstract MatcherEvalContext<AttributeId> startContext(Object instance, Set<String> knownTypes);

   protected abstract FilterProcessingChain<?> createFilterProcessingChain(Map<String, Object> namedParameters);

   protected abstract FilterRegistry<AttributeId> createFilterRegistryForType(TypeMetadata typeMetadata);
}