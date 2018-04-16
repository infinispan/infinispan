package org.infinispan.objectfilter.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextVisitor;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParser;
import org.infinispan.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.query.dsl.Query;
import org.jboss.logging.Logger;

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

   private final Map<TypeMetadata, FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>> filtersByType = new HashMap<>();

   private final Map<TypeMetadata, FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>> deltaFiltersByType = new HashMap<>();

   protected final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   protected BaseMatcher(ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.propertyHelper = propertyHelper;
   }

   public ObjectPropertyHelper<TypeMetadata> getPropertyHelper() {
      return propertyHelper;
   }

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
         MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> ctx = startMultiTypeContext(false, userContext, eventType, instance);
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
   public void matchDelta(Object userContext, Object eventType, Object instanceOld, Object instanceNew, Object joiningEvent, Object updatedEvent, Object leavingEvent) {
      if (instanceOld == null && instanceNew == null) {
         throw new IllegalArgumentException("instances cannot be both null");
      }

      read.lock();
      try {
         MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> ctx1 = null;
         MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> ctx2 = null;

         if (instanceOld != null) {
            ctx1 = startMultiTypeContext(true, userContext, eventType, instanceOld);
            if (ctx1 != null) {
               // try to match
               ctx1.process(ctx1.getRootNode());
            }
         }

         if (instanceNew != null) {
            ctx2 = startMultiTypeContext(true, userContext, eventType, instanceNew);
            if (ctx2 != null) {
               // try to match
               ctx2.process(ctx2.getRootNode());
            }
         }

         if (ctx1 != null) {
            // notify
            ctx1.notifyDeltaSubscribers(ctx2, joiningEvent, updatedEvent, leavingEvent);
         } else if (ctx2 != null) {
            // notify
            ctx2.notifyDeltaSubscribers(null, leavingEvent, updatedEvent, joiningEvent);
         }
      } finally {
         read.unlock();
      }
   }

   @Override
   public ObjectFilter getObjectFilter(Query query) {
      return getObjectFilter(query.getQueryString(), null);
   }

   @Override
   public ObjectFilter getObjectFilter(String queryString) {
      return getObjectFilter(queryString, null);
   }

   @Override
   public ObjectFilter getObjectFilter(String queryString, List<FieldAccumulator> acc) {
      final IckleParsingResult<TypeMetadata> parsingResult = IckleParser.parse(queryString, propertyHelper);
      disallowGroupingAndAggregations(parsingResult);

      // if the query is a contradiction just return an ObjectFilter that rejects everything
      if (parsingResult.getWhereClause() == ConstantBooleanExpr.FALSE) {
         return new RejectObjectFilter<>(null, parsingResult);
      }

      final MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter = createMetadataAdapter(parsingResult.getTargetEntityMetadata());

      // if the query is a tautology or there is no query at all and there is no sorting or projections just return a special instance that accepts anything
      // in case we have sorting and projections we cannot take this shortcut because the computation of projections or sort projections is a bit more involved
      if ((parsingResult.getWhereClause() == null || parsingResult.getWhereClause() == ConstantBooleanExpr.TRUE)
            && parsingResult.getSortFields() == null && parsingResult.getProjectedPaths() == null) {
         return new AcceptObjectFilter<>(null, this, metadataAdapter, parsingResult);
      }

      FieldAccumulator[] accumulators = acc != null ? acc.toArray(new FieldAccumulator[acc.size()]) : null;
      return new ObjectFilterImpl<>(this, metadataAdapter, parsingResult, accumulators);
   }

   @Override
   public ObjectFilter getObjectFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscriptionImpl = (FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId>) filterSubscription;
      ObjectFilter objectFilter = getObjectFilter(filterSubscriptionImpl.getQueryString());
      return filterSubscriptionImpl.getNamedParameters() != null ? objectFilter.withParameters(filterSubscriptionImpl.getNamedParameters()) : objectFilter;
   }

   @Override
   public FilterSubscription registerFilter(Query query, FilterCallback callback, Object... eventType) {
      return registerFilter(query, callback, false, eventType);
   }

   @Override
   public FilterSubscription registerFilter(Query query, FilterCallback callback, boolean isDeltaFilter, Object... eventType) {
      return registerFilter(query.getQueryString(), query.getParameters(), callback, isDeltaFilter, eventType);
   }

   @Override
   public FilterSubscription registerFilter(String queryString, FilterCallback callback, Object... eventType) {
      return registerFilter(queryString, callback, false, eventType);
   }

   @Override
   public FilterSubscription registerFilter(String queryString, FilterCallback callback, boolean isDeltaFilter, Object... eventType) {
      return registerFilter(queryString, null, callback, isDeltaFilter, eventType);
   }

   @Override
   public FilterSubscription registerFilter(String queryString, Map<String, Object> namedParameters,
                                            FilterCallback callback, Object... eventType) {
      return registerFilter(queryString, namedParameters, callback, false, eventType);
   }

   @Override
   public FilterSubscription registerFilter(String queryString, Map<String, Object> namedParameters,
                                            FilterCallback callback, boolean isDeltaFilter, Object... eventType) {
      IckleParsingResult<TypeMetadata> parsingResult = IckleParser.parse(queryString, propertyHelper);
      disallowGroupingAndAggregations(parsingResult);
      disallowFullText(parsingResult);

      Map<TypeMetadata, FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>> filterMap = isDeltaFilter ? deltaFiltersByType : filtersByType;
      write.lock();
      try {
         FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry = filterMap.get(parsingResult.getTargetEntityMetadata());
         if (filterRegistry == null) {
            filterRegistry = new FilterRegistry<>(createMetadataAdapter(parsingResult.getTargetEntityMetadata()), true);
            filterMap.put(filterRegistry.getMetadataAdapter().getTypeMetadata(), filterRegistry);
         }
         return filterRegistry.addFilter(queryString, namedParameters, parsingResult.getWhereClause(), parsingResult.getProjections(), parsingResult.getProjectedTypes(), parsingResult.getSortFields(), callback, isDeltaFilter, eventType);
      } finally {
         write.unlock();
      }
   }

   private void disallowFullText(IckleParsingResult<TypeMetadata> parsingResult) {
      if (parsingResult.getWhereClause() != null) {
         if (parsingResult.getWhereClause().acceptVisitor(FullTextVisitor.INSTANCE)) {
            throw log.getFiltersCannotUseFullTextSearchException();
         }
      }
   }

   private void disallowGroupingAndAggregations(IckleParsingResult<TypeMetadata> parsingResult) {
      if (parsingResult.hasGroupingOrAggregations()) {
         throw log.getFiltersCannotUseGroupingOrAggregationException();
      }
   }

   @Override
   public void unregisterFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId> filterSubscriptionImpl = (FilterSubscriptionImpl<TypeMetadata, AttributeMetadata, AttributeId>) filterSubscription;
      Map<TypeMetadata, FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId>> filterMap = filterSubscription.isDeltaFilter() ? deltaFiltersByType : filtersByType;
      write.lock();
      try {
         FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> filterRegistry = filterMap.get(filterSubscriptionImpl.getMetadataAdapter().getTypeMetadata());
         if (filterRegistry != null) {
            filterRegistry.removeFilter(filterSubscription);
         } else {
            throw new IllegalStateException("No filter was found for type " + filterSubscriptionImpl.getMetadataAdapter().getTypeMetadata());
         }
         if (filterRegistry.getFilterSubscriptions().isEmpty()) {
            filterMap.remove(filterRegistry.getMetadataAdapter().getTypeMetadata());
         }
      } finally {
         write.unlock();
      }
   }

   /**
    * Creates a new {@link MatcherEvalContext} capable of dealing with multiple filters. The context is created only if
    * the given instance is recognized to be of a type that has some filters registered. If there are no filters, {@code
    * null} is returned to signal this condition and make the evaluation faster. This method must be called while
    * holding the internal write lock.
    *
    * @param isDelta     indicates if this is a delta match of not
    * @param userContext an opaque value, possibly null, the is received from the caller and is to be handed to the
    *                    {@link FilterCallback} in case a match is detected
    * @param eventType   on optional event type discriminator
    * @param instance    the instance to filter; never {@code null}
    * @return the MatcherEvalContext or {@code null} if no filter was registered for the instance
    */
   protected abstract MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> startMultiTypeContext(boolean isDelta, Object userContext, Object eventType, Object instance);

   /**
    * Creates a new {@link MatcherEvalContext} capable of dealing with a single filter for a single type. The context is
    * created only if the given instance is recognized to be of a type that has some filters registered. If there are no
    * filters, {@code null} is returned to signal this condition and make the evaluation faster. This method must be
    * called while holding the internal write lock.
    *
    * @param userContext     an opaque value, possibly null, the is received from the caller and is to be handed to the
    *                        {@link FilterCallback} in case a match is detected
    * @param instance        the instance to filter; never {@code null}
    * @param metadataAdapter the metadata adapter of expected instance type
    * @return the MatcherEvalContext or {@code null} if no filter was registered for the instance
    */
   protected abstract MatcherEvalContext<TypeMetadata, AttributeMetadata, AttributeId> startSingleTypeContext(Object userContext, Object eventType, Object instance, MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> metadataAdapter);

   protected abstract MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> createMetadataAdapter(TypeMetadata entityType);

   protected final FilterRegistry<TypeMetadata, AttributeMetadata, AttributeId> getFilterRegistryForType(boolean isDeltaFilter, TypeMetadata entityType) {
      return isDeltaFilter ? deltaFiltersByType.get(entityType) : filtersByType.get(entityType);
   }

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
