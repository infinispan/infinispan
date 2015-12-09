package org.infinispan.objectfilter.impl;

import org.hibernate.hql.QueryParser;
import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.jboss.logging.Logger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
//todo [anistor] make package local
public abstract class BaseMatcher<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> implements Matcher {

   private static final Log log = Logger.getMessageLogger(Log.class, BaseMatcher.class.getName());

   private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

   private final Lock read = readWriteLock.readLock();

   private final Lock write = readWriteLock.writeLock();

   private final QueryParser queryParser = new QueryParser();

   protected final Map<String, FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>> filtersByTypeName = new HashMap<String, FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>>();

   protected final Map<TypeMetadata, FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>> filtersByType = new HashMap<TypeMetadata, FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>>();

   /**
    * Executes the registered filters and notifies each one of them whether it was satisfied or not by the given
    * instance.
    *
    * @param userContext an optional user provided object to be passed to matching subscribers along with the matching
    *                    instance; can be {@code null}
    * @param eventType   on optional event type discriminator that is matched against the even type specified when the
    *                    filter was registered; can be {@code null}
    * @param instance    the object to test against the registered filters; never {@code null}
    */
   @Override
   public void match(Object userContext, Object eventType, Object instance) {
      if (instance == null) {
         throw new IllegalArgumentException("instance cannot be null");
      }

      read.lock();
      try {
         MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> ctx = startMultiTypeContext(userContext, eventType, instance);
         if (ctx != null) {
            // try to match
            ctx.process(ctx.getRootNode());

            // notify
            ctx.notifySubscribers();
         }
      } finally {
         read.unlock();
      }
   }

   @Override
   public void matchDelta(Object userContext, Object eventType, Object instanceOld, Object instanceNew, Object joiningEvent, Object leavingEvent) {
      if (instanceOld == null && instanceNew == null) {
         throw new IllegalArgumentException("instances cannot be both null");
      }

      read.lock();
      try {
         MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> ctx1 = null;
         MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> ctx2 = null;

         if (instanceOld != null) {
            ctx1 = startMultiTypeContext(userContext, eventType, instanceOld);
            if (ctx1 != null) {
               // try to match
               ctx1.process(ctx1.getRootNode());
            }
         }

         if (instanceNew != null) {
            ctx2 = startMultiTypeContext(userContext, eventType, instanceNew);
            if (ctx2 != null) {
               // try to match
               ctx2.process(ctx2.getRootNode());
            }
         }

         if (ctx1 != null) {
            // notify
            ctx1.notifyDeltaSubscribers(ctx2, joiningEvent, leavingEvent);
         } else if (ctx2 != null) {
            // notify
            ctx2.notifyDeltaSubscribers(null, leavingEvent, joiningEvent);
         }
      } finally {
         read.unlock();
      }
   }

   @Override
   public ObjectFilter getObjectFilter(Query query) {
      BaseQuery baseQuery = (BaseQuery) query;
      return getObjectFilter(baseQuery.getJPAQuery(), baseQuery.getNamedParameters());
   }

   @Override
   public ObjectFilter getObjectFilter(String jpaQuery) {
      return getObjectFilter(jpaQuery, null);
   }

   @Override
   public ObjectFilter getObjectFilter(String jpaQuery, Map<String, Object> namedParameters) {
      return getObjectFilter(jpaQuery, namedParameters, null);
   }

   @Override
   public ObjectFilter getObjectFilter(String jpaQuery, Map<String, Object> namedParameters, List<FieldAccumulator> acc) {
      final FilterParsingResult<TypeMetadata> parsingResult = parse(jpaQuery, namedParameters);
      disallowGroupingAndAggregations(parsingResult);

      // if the query is a contradiction just return an ObjectFilter that rejects everything
      if (parsingResult.getWhereClause() == ConstantBooleanExpr.FALSE) {
         return new ObjectFilter() {

            @Override
            public String getEntityTypeName() {
               return parsingResult.getTargetEntityName();
            }

            @Override
            public String[] getProjection() {
               return null;
            }

            @Override
            public Class<?>[] getProjectionTypes() {
               return null;
            }

            @Override
            public SortField[] getSortFields() {
               return null;
            }

            @Override
            public Comparator<Comparable[]> getComparator() {
               return null;
            }

            @Override
            public FilterResult filter(Object instance) {
               if (instance == null) {
                  throw new IllegalArgumentException("instance cannot be null");
               }
               return null;
            }
         };
      }

      final MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter = createMetadataAdapter(parsingResult.getTargetEntityMetadata());

      // if the query is a tautology or there is no query at all and there is no sorting or projections just return a special instance that accepts anything
      // in case we have sorting and projections we cannot take this shortcut because the computation of projections or sort projections is a bit more involved
      if ((parsingResult.getWhereClause() == null || parsingResult.getWhereClause() == ConstantBooleanExpr.TRUE)
            && parsingResult.getSortFields() == null && parsingResult.getProjectedPaths() == null) {
         return new ObjectFilter() {

            @Override
            public String getEntityTypeName() {
               return parsingResult.getTargetEntityName();
            }

            @Override
            public String[] getProjection() {
               return null;
            }

            @Override
            public Class<?>[] getProjectionTypes() {
               return null;
            }

            @Override
            public SortField[] getSortFields() {
               return null;
            }

            @Override
            public Comparator<Comparable[]> getComparator() {
               return null;
            }

            @Override
            public FilterResult filter(Object instance) {
               if (instance == null) {
                  throw new IllegalArgumentException("instance cannot be null");
               }
               MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> matcherEvalContext = startSingleTypeContext(null, null, instance, metadataAdapter);
               if (matcherEvalContext != null) {
                  // once we have a successfully created context we already have a match as there are no filter conditions except for entity type
                  return new FilterResultImpl(convert(instance), null, null);
               }
               return null;
            }
         };
      }

      FieldAccumulator[] accumulators = acc != null ? acc.toArray(new FieldAccumulator[acc.size()]) : null;
      return new ObjectFilterImpl<TypeMetadata, AttributeMetadata, AttributeId>(this, metadataAdapter, jpaQuery, namedParameters,
            parsingResult.getWhereClause(), parsingResult.getProjections(), parsingResult.getProjectedTypes(), parsingResult.getSortFields(), accumulators);
   }

   @Override
   public ObjectFilter getObjectFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscriptionImpl = (FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId>) filterSubscription;
      return getObjectFilter(filterSubscriptionImpl.getQueryString(), filterSubscriptionImpl.getNamedParameters());
   }

   @Override
   public FilterSubscription registerFilter(Query query, FilterCallback callback, Object... eventType) {
      BaseQuery baseQuery = (BaseQuery) query;
      return registerFilter(baseQuery.getJPAQuery(), baseQuery.getNamedParameters(), callback, eventType);
   }

   @Override
   public FilterSubscription registerFilter(String jpaQuery, FilterCallback callback, Object... eventType) {
      return registerFilter(jpaQuery, null, callback, eventType);
   }

   @Override
   public FilterSubscription registerFilter(String jpaQuery, Map<String, Object> namedParameters, FilterCallback callback, Object... eventType) {
      FilterParsingResult<TypeMetadata> parsingResult = parse(jpaQuery, namedParameters);
      disallowGroupingAndAggregations(parsingResult);

      write.lock();
      try {
         FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry = filtersByTypeName.get(parsingResult.getTargetEntityName());
         if (filterRegistry == null) {
            filterRegistry = new FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>(createMetadataAdapter(parsingResult.getTargetEntityMetadata()), true);
            filtersByTypeName.put(parsingResult.getTargetEntityName(), filterRegistry);
            filtersByType.put(filterRegistry.getMetadataAdapter().getTypeMetadata(), filterRegistry);
         }
         return filterRegistry.addFilter(jpaQuery, namedParameters, parsingResult.getWhereClause(), parsingResult.getProjections(), parsingResult.getProjectedTypes(), parsingResult.getSortFields(), callback, eventType);
      } finally {
         write.unlock();
      }
   }

   public FilterParsingResult<TypeMetadata> parse(String jpaQuery, Map<String, Object> namedParameters) {
      return queryParser.parseQuery(jpaQuery, createFilterProcessingChain(namedParameters));
   }

   private void disallowGroupingAndAggregations(FilterParsingResult<TypeMetadata> parsingResult) {
      if (parsingResult.hasGroupingOrAggregations()) {
         throw log.getFiltersCannotUseGroupingOrAggregationException();
      }
   }

   @Override
   public void unregisterFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl filterSubscriptionImpl = (FilterSubscriptionImpl) filterSubscription;
      write.lock();
      try {
         FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry = filtersByTypeName.get(filterSubscriptionImpl.getEntityTypeName());
         if (filterRegistry != null) {
            filterRegistry.removeFilter(filterSubscription);
         } else {
            throw new IllegalStateException("Reached illegal state");
         }
         if (filterRegistry.getFilterSubscriptions().isEmpty()) {
            filtersByTypeName.remove(filterRegistry.getMetadataAdapter().getTypeName());
            filtersByType.remove(filterRegistry.getMetadataAdapter().getTypeMetadata());
         }
      } finally {
         write.unlock();
      }
   }

   /**
    * Creates a new {@link MatcherEvalContext} capable of dealing with multiple filters. The context is created only if
    * the given instance is recognized to be of a type that has some filters registered. If there are no filters, {@code
    * null} is returned to signal this condition and make the evaluation faster. This method is called while holding the
    * internal write lock.
    *
    * @param userContext an opaque value, possibly null, the is received from the caller and is to be handed to the
    *                    {@link FilterCallback} in case a match is detected
    * @param eventType   on optional event type discriminator
    * @param instance    the instance to filter; never {@code null}
    * @return the MatcherEvalContext or {@code null} if no filter was registered for the instance
    */
   protected abstract MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> startMultiTypeContext(Object userContext, Object eventType, Object instance);

   /**
    * Creates a new {@link MatcherEvalContext} capable of dealing with a single filter for a single type. The context is
    * created only if the given instance is recognized to be of a type that has some filters registered. If there are no
    * filters, {@code null} is returned to signal this condition and make the evaluation faster. This method is called
    * while holding the internal write lock.
    *
    * @param userContext     an opaque value, possibly null, the is received from the caller and is to be handed to the
    *                        {@link FilterCallback} in case a match is detected
    * @param instance        the instance to filter; never {@code null}
    * @param metadataAdapter the metadata adapter of expected instance type
    * @return the MatcherEvalContext or {@code null} if no filter was registered for the instance
    */
   protected abstract MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> startSingleTypeContext(Object userContext, Object eventType, Object instance, MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter);

   protected abstract FilterProcessingChain<TypeMetadata> createFilterProcessingChain(Map<String, Object> namedParameters);

   public abstract ObjectPropertyHelper<TypeMetadata> getPropertyHelper();

   protected abstract MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> createMetadataAdapter(TypeMetadata entityType);

   protected abstract FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> getFilterRegistryForType(TypeMetadata entityType);

   /**
    * Decorates a matching instance before it is presented to the caller of the {@link ObjectFilter#filter(Object)}.
    *
    * @param instance never null
    * @return the converted/decorated instance
    */
   protected Object convert(Object instance) {
      return instance;
   }
}
