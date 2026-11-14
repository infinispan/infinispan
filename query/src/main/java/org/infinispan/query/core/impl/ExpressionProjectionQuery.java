package org.infinispan.query.core.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.query.BaseQuery;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.objectfilter.impl.ql.ExpressionProjection;
import org.infinispan.query.objectfilter.impl.ql.PropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.update.QueryFunctionRegistry;
import org.infinispan.query.objectfilter.impl.syntax.update.UpdateValueEvaluator;
import org.infinispan.query.objectfilter.impl.syntax.update.UpdateValueExpr;

/**
 * A query wrapper that fetches entities from an inner query and evaluates
 * expression projections (function calls) against each entity in-memory.
 * <p>
 * Used when the SELECT clause contains function calls that cannot be pushed
 * down to the search index.
 *
 * @since 16.4
 */
public final class ExpressionProjectionQuery extends BaseQuery<Object> {

   private final BaseQuery<?> innerQuery;
   private final List<PropertyPath<?>> projections;
   private final QueryFunctionRegistry functionRegistry;
   private final ImmutableSerializationContext serCtx;
   private final Map<String, Pattern> patternCache = new ConcurrentHashMap<>();
   private final UpdateQueryHelper.ReflectionResolver propertyResolver = new UpdateQueryHelper.ReflectionResolver();

   public ExpressionProjectionQuery(String queryString,
                                    Map<String, Object> namedParameters,
                                    BaseQuery<?> innerQuery,
                                    List<PropertyPath<?>> projections,
                                    QueryFunctionRegistry functionRegistry,
                                    ImmutableSerializationContext serCtx,
                                    long startOffset,
                                    int maxResults,
                                    boolean local) {
      super(queryString, namedParameters, null, startOffset, maxResults, local);
      this.innerQuery = innerQuery;
      this.projections = List.copyOf(projections);
      this.functionRegistry = functionRegistry;
      this.serCtx = serCtx;
      for (PropertyPath<?> projection : projections) {
         if (projection instanceof ExpressionProjection<?> ep
               && ep.getExpression() instanceof UpdateValueExpr expr) {
            expr.prepare(functionRegistry, patternCache);
         }
      }
   }

   @Override
   public void resetQuery() {
      innerQuery.resetQuery();
   }

   @Override
   @SuppressWarnings("deprecation")
   public int getResultSize() {
      return innerQuery.getResultSize();
   }

   @Override
   public Query<Object> setParameter(String paramName, Object paramValue) {
      // Update (and validate against) our own parameters, which the projections are evaluated with,
      // and keep the inner query in step instead of relying on both sharing the same map instance.
      super.setParameter(paramName, paramValue);
      innerQuery.setParameter(paramName, paramValue);
      return this;
   }

   @Override
   public Query<Object> setParameters(Map<String, Object> paramValues) {
      super.setParameters(paramValues);
      innerQuery.setParameters(paramValues);
      return this;
   }

   @Override
   public List<Object> list() {
      QueryResult<?> innerResult = innerQuery.execute();
      List<Object> mapped = new ArrayList<>();
      for (Object entity : innerResult.list()) {
         mapped.add(evaluateProjections(entity));
      }
      return mapped;
   }

   @Override
   public QueryResult<Object> execute() {
      QueryResult<?> innerResult = innerQuery.execute();
      List<Object> mapped = new ArrayList<>();
      for (Object entity : innerResult.list()) {
         mapped.add(evaluateProjections(entity));
      }
      return new QueryResultImpl<>(innerResult.count(), mapped);
   }

   @Override
   public CompletionStage<org.infinispan.commons.api.query.QueryResult<Object>> executeAsync() {
      throw new UnsupportedOperationException("Not supported for expression projection queries");
   }

   @Override
   public CompletionStage<Integer> executeStatementAsync() {
      throw new UnsupportedOperationException("Not supported for expression projection queries");
   }

   @Override
   public CloseableIterator<Object> iterator() {
      CloseableIterator<?> inner = innerQuery.iterator();
      return new CloseableIterator<>() {
         @Override
         public boolean hasNext() {
            return inner.hasNext();
         }

         @Override
         public Object next() {
            return evaluateProjections(inner.next());
         }

         @Override
         public void close() {
            inner.close();
         }
      };
   }

   @Override
   public Query<Object> startOffset(long startOffset) {
      super.startOffset(startOffset);
      innerQuery.startOffset(getStartOffset());
      return this;
   }

   @Override
   public Query<Object> maxResults(int maxResults) {
      super.maxResults(maxResults);
      innerQuery.maxResults(getMaxResults());
      return this;
   }

   @Override
   public Query<Object> local(boolean local) {
      super.local(local);
      innerQuery.local(local);
      return this;
   }

   @Override
   public Query<Object> scoreRequired(boolean scoreRequired) {
      super.scoreRequired(scoreRequired);
      innerQuery.scoreRequired(scoreRequired);
      return this;
   }

   @Override
   public Query<Object> timeout(long timeout, TimeUnit timeUnit) {
      super.timeout(timeout, timeUnit);
      innerQuery.timeout(timeout, timeUnit);
      return this;
   }

   @Override
   public Query<Object> hitCountAccuracy(int hitCountAccuracy) {
      super.hitCountAccuracy(hitCountAccuracy);
      innerQuery.hitCountAccuracy(hitCountAccuracy);
      return this;
   }

   @Override
   public <K> ClosableIteratorWithCount<EntityEntry<K, Object>> entryIterator(boolean withMetadata) {
      throw new UnsupportedOperationException("Not supported for expression projection queries");
   }

   @Override
   public int executeStatement() {
      throw new UnsupportedOperationException("Not supported for expression projection queries");
   }

   /**
    * Evaluates all projections for one entity.
    * <p>
    * Like the rest of the query pipeline, a projection query always yields an
    * {@code Object[]} row with one slot per projection item, even when there
    * is a single projection: the remote and REST serializers, the client-side
    * row reconstitution, and the embedded {@code FilterResult} contract all
    * rely on that invariant.
    */
   private Object[] evaluateProjections(Object entity) {
      QueryFunctionContext ctx = createContext(entity);
      Object[] results = new Object[projections.size()];
      for (int i = 0; i < projections.size(); i++) {
         results[i] = evaluateOne(projections.get(i), ctx);
      }
      return results;
   }

   private QueryFunctionContext createContext(Object entity) {
      if (entity instanceof byte[] bytes && serCtx != null) {
         try {
            Function<String[], Object> reader = UpdateQueryHelper.createProtobufPropertyReader(serCtx, bytes);
            return new QueryFunctionContext(entity, reader, namedParameters, functionRegistry, patternCache);
         } catch (Exception e) {
            throw new IllegalArgumentException("Cannot read protobuf entity", e);
         }
      }
      if (entity instanceof WrappedByteArray wrapped && serCtx != null) {
         try {
            Function<String[], Object> reader = UpdateQueryHelper.createProtobufPropertyReader(serCtx, wrapped.getBytes());
            return new QueryFunctionContext(entity, reader, namedParameters, functionRegistry, patternCache);
         } catch (Exception e) {
            throw new IllegalArgumentException("Cannot read protobuf entity", e);
         }
      }
      return new QueryFunctionContext(entity, path -> {
         try {
            return propertyResolver.readNestedProperty(entity, path);
         } catch (Exception e) {
            throw new IllegalArgumentException("Cannot read property path: " + String.join(".", path), e);
         }
      }, namedParameters, functionRegistry, patternCache);
   }

   private Object evaluateOne(PropertyPath<?> projection, QueryFunctionContext ctx) {
      if (projection instanceof ExpressionProjection<?> ep) {
         return UpdateValueEvaluator.evaluateOperand(ep.getExpression(), ctx);
      }
      // A plain property projection in a mixed select (e.g. `select name,
      // upper(name)`): read it straight from the entity.
      if (projection.getLength() == 1 && projection.isAlias()) {
         return ctx.getEntity();
      }
      String[] path = projection.asArrayPath();
      if (path.length == 1 && CacheValuePropertyPath.VALUE_PROPERTY_NAME.equals(path[0])) {
         return ctx.getEntity();
      }
      return ctx.getPropertyValue(path);
   }
}
