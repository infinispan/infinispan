package org.infinispan.objectfilter;

import org.hibernate.hql.QueryParser;
import org.infinispan.objectfilter.impl.FilterRegistry;
import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
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
public abstract class BaseMatcher<TypeMetadata, AttributeId extends Comparable<AttributeId>> implements Matcher {

   private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

   private final Lock read = readWriteLock.readLock();

   private final Lock write = readWriteLock.writeLock();

   private final QueryParser queryParser = new QueryParser();

   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   private final Map<String, FilterRegistry<AttributeId>> filtersByType = new HashMap<String, FilterRegistry<AttributeId>>();

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

   public Matcher getSingleFilterMatcher(FilterSubscription filterSubscription, final FilterCallback filterCallback) {
      final FilterSubscriptionImpl filterSubscriptionImpl = (FilterSubscriptionImpl) filterSubscription;
      final Set<String> types = Collections.singleton(filterSubscriptionImpl.getEntityTypeName());
      final PredicateIndex<AttributeId> predicateIndex = new PredicateIndex<AttributeId>();

      for (BENode node : filterSubscriptionImpl.getBETree().getNodes()) {
         if (node instanceof PredicateNode) {
            final PredicateNode<AttributeId> predicateNode = (PredicateNode<AttributeId>) node;
            Predicate.Callback predicateCallback = new Predicate.Callback() {
               @Override
               public void handleValue(MatcherEvalContext<?> ctx, boolean isMatching) {
                  FilterEvalContext context = ctx.getFilterContext(filterSubscriptionImpl);
                  predicateNode.handleChildValue(predicateNode, isMatching, context);
               }
            };
            predicateIndex.addSubscriptionForPredicate(predicateNode, predicateCallback);
         }
      }

      return new Matcher() {
         @Override
         public void match(Object instance) {
            MatcherEvalContext<AttributeId> ctx = startContext(instance, types);
            if (ctx != null) {
               ctx.process(predicateIndex.getRoot());

               // TODO [anistor] the instance here can be a byte[] or stream if the payload is protobuf encoded
               filterCallback.onFilterResult(ctx.getInstance(), null, ctx.getFilterContext(filterSubscriptionImpl).getResult());   //todo projection
            }
         }
      };
   }

   public FilterSubscription registerFilter(Query query, FilterCallback callback) {
      if (!(query instanceof FilterQuery)) {
         throw new IllegalArgumentException("The Query object must be created by a QueryFactory returned by this Matcher");
      }
      FilterQuery filterQuery = (FilterQuery) query;
      return registerFilter(filterQuery.getJpqlString(), callback);
   }

   public FilterSubscription registerFilter(String jpaQuery, final FilterCallback callback) {
      FilterParsingResult<TypeMetadata> parsingResult = queryParser.parseQuery(jpaQuery, createFilterProcessingChain(null)); //todo [anistor] query params not yet supported
      BooleanExpr normalizedFilter = booleanFilterNormalizer.normalize(parsingResult.getQuery());

      write.lock();
      try {
         FilterRegistry filterRegistry = filtersByType.get(parsingResult.getTargetEntityName());
         if (filterRegistry == null) {
            filterRegistry = createFilterRegistryForType(parsingResult.getTargetEntityMetadata());
            filtersByType.put(parsingResult.getTargetEntityName(), filterRegistry);
         }

         return filterRegistry.addFilter(normalizedFilter, parsingResult.getProjections(), callback);
      } finally {
         write.unlock();
      }
   }

   public void unregisterFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl filterSubscriptionImpl = (FilterSubscriptionImpl) filterSubscription;
      write.lock();
      try {
         FilterRegistry filterRegistry = filtersByType.get(filterSubscriptionImpl.getEntityTypeName());
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
    * Creates a new MatcherEvalContext only if the given instances has any registered filters. This method is called
    * while holding the write lock.
    *
    * @param instance
    * @param knownTypes
    * @return the context or null if no filter was registered for the instance
    */
   protected abstract MatcherEvalContext<AttributeId> startContext(Object instance, Set<String> knownTypes);

   protected abstract FilterProcessingChain<?> createFilterProcessingChain(Map<String, Object> namedParameters);

   protected abstract FilterRegistry<AttributeId> createFilterRegistryForType(TypeMetadata typeMetadata);
}