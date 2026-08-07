package org.infinispan.query.core.impl;

import static org.infinispan.query.core.impl.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheStream;
import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.CacheFilters;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufFieldUpdater;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.protostream.impl.TagReaderImpl;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.objectfilter.ObjectFilter;
import org.infinispan.query.objectfilter.impl.syntax.parser.IckleParsingResult;


/**
 * Non-indexed embedded-mode query.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class EmbeddedQuery<T> extends BaseEmbeddedQuery<T> {

   private final QueryEngine<?> queryEngine;

   private IckleFilterAndConverter<?, ?> filter;

   private final int defaultMaxResults;

   private final List<IckleParsingResult.UpdateOperation> updateOperations;

   public EmbeddedQuery(QueryEngine<?> queryEngine, AdvancedCache<?, ?> cache,
                        String queryString, IckleParsingResult.StatementType statementType,
                        Map<String, Object> namedParameters, String[] projection,
                        long startOffset, int maxResults, int defaultMaxResults, LocalQueryStatistics queryStatistics, boolean local,
                        List<IckleParsingResult.UpdateOperation> updateOperations) {
      super(cache, queryString, statementType, namedParameters, projection, startOffset, maxResults, queryStatistics, local);
      this.queryEngine = queryEngine;
      this.defaultMaxResults = defaultMaxResults;
      this.updateOperations = updateOperations;

      if (maxResults == -1) {
         // apply the default
         super.maxResults = defaultMaxResults;
      }
   }

   @Override
   public void resetQuery() {
      super.resetQuery();
      filter = null;
   }

   @Override
   protected void recordQuery(long time) {
      queryStatistics.nonIndexedQueryExecuted(queryString, time);
   }

   private IckleFilterAndConverter<?, ?> createFilter() {
      // filter is created first time only, or again if resetQuery was called meanwhile
      if (filter == null) {
         filter = queryEngine.createAndWireFilter(queryString, namedParameters);

         // force early query validation, at creation time, rather than deferring to execution time
         filter.getObjectFilter();
      }
      return filter;
   }

   @Override
   protected Comparator<Comparable<?>[]> getComparator() {
      return createFilter().getObjectFilter().getComparator();
   }

   @Override
   protected ClosableIteratorWithCount<ObjectFilter.FilterResult> getInternalIterator() {
      IckleFilterAndConverter<Object, Object> ickleFilter = (IckleFilterAndConverter<Object, Object>) createFilter();
      AdvancedCache<Object, Object> cache = (AdvancedCache<Object, Object>) (isLocal() ? this.cache.withFlags(Flag.CACHE_MODE_LOCAL) : this.cache);

      CacheStream<CacheEntry<Object, Object>> entryStream = cache.cacheEntrySet().stream();
      if (timeout > 0) {
         entryStream = entryStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }
      CacheStream<ObjectFilter.FilterResult> resultStream = CacheFilters.filterAndConvertToValue(entryStream, ickleFilter);
      if (timeout > 0) {
         resultStream = resultStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }
      return Closeables.iteratorWithCount(resultStream);
   }

   @Override
   public QueryResult<T> execute() {
      if (isSelectStatement()) {
         return super.execute();
      }

      return new QueryResultImpl<>(executeStatement(), Collections.emptyList());
   }

   @Override
   public int executeStatement() {
      if (isSelectStatement()) {
         throw CONTAINER.unsupportedStatement();
      }

      if (getStartOffset() != 0 || getMaxResults() != defaultMaxResults) {
         throw CONTAINER.statementCannotUsePaging();
      }

      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      IckleFilterAndConverter<Object, Object> ickleFilter = (IckleFilterAndConverter<Object, Object>) createFilter();
      AdvancedCache<Object, Object> cache = (AdvancedCache<Object, Object>) (isLocal() ? this.cache.withFlags(Flag.CACHE_MODE_LOCAL) : this.cache);

      CacheStream<CacheEntry<Object, Object>> entryStream = cache.cacheEntrySet().stream();
      if (timeout > 0) {
         entryStream = entryStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }

      CacheStream<?> filteredKeyStream = CacheFilters.filterAndConvertToKey(entryStream, ickleFilter);
      if (timeout > 0) {
         filteredKeyStream = filteredKeyStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }

      Optional<Integer> count;
      if (statementType == IckleParsingResult.StatementType.UPDATE) {
         count = filteredKeyStream.map(new UpdateFunction(updateOperations)).reduce(Integer::sum);
      } else {
         count = filteredKeyStream.map(new DeleteFunction()).reduce(Integer::sum);
      }
      filteredKeyStream.close();

      if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

      return count.orElse(0);
   }

   @ProtoTypeId(ProtoStreamTypeIds.ICKLE_DELETE_FUNCTION)
   @Scope(Scopes.NONE)
   static final class DeleteFunction implements Function<Object, Integer> {

      @Inject
      AdvancedCache<?,?> cache;

      @ProtoFactory
      DeleteFunction() {}

      @Override
      public Integer apply(Object key) {
         return cache.withStorageMediaType().remove(key) == null ? 0 : 1;
      }
   }

   @Scope(Scopes.NONE)
   static final class UpdateFunction implements Function<Object, Integer> {

      @Inject
      AdvancedCache<?, ?> cache;

      @Inject
      SerializationContextRegistry ctxRegistry;

      private final List<IckleParsingResult.UpdateOperation> updateOperations;

      UpdateFunction(List<IckleParsingResult.UpdateOperation> updateOperations) {
         this.updateOperations = updateOperations;
      }

      @Override
      public Integer apply(Object key) {
         AdvancedCache<Object, Object> storageCache =
               (AdvancedCache<Object, Object>) cache.withStorageMediaType();
         Object existingValue = storageCache.get(key);
         if (existingValue == null) return 0;

         try {
            ImmutableSerializationContext serCtx = ctxRegistry.getUserCtx();
            byte[] wrappedBytes = extractBytes(existingValue);

            String typeName = null;
            Integer typeId = null;
            byte[] innerBytes = null;

            TagReaderImpl reader = TagReaderImpl.newInstance(serCtx, wrappedBytes);
            int tag;
            while ((tag = reader.readTag()) != 0) {
               int fieldNumber = WireType.getTagFieldNumber(tag);
               if (fieldNumber == WrappedMessage.WRAPPED_TYPE_NAME) {
                  typeName = reader.readString();
               } else if (fieldNumber == WrappedMessage.WRAPPED_TYPE_ID) {
                  typeId = reader.readUInt32();
               } else if (fieldNumber == WrappedMessage.WRAPPED_MESSAGE) {
                  innerBytes = reader.readByteArray();
               } else {
                  reader.skipField(tag);
               }
            }

            if (innerBytes == null) return 0;

            String resolvedTypeName = typeName != null ? typeName
                  : serCtx.getDescriptorByTypeId(typeId).getFullName();
            Descriptor descriptor = serCtx.getMessageDescriptor(resolvedTypeName);

            List<ProtobufFieldUpdater.UpdateOperation> ops = new ArrayList<>();
            for (IckleParsingResult.UpdateOperation uo : updateOperations) {
               ProtobufFieldUpdater.OperationType opType = switch (uo.getType()) {
                  case SET -> ProtobufFieldUpdater.OperationType.SET;
                  case ADD -> ProtobufFieldUpdater.OperationType.ADD;
                  case REMOVE -> ProtobufFieldUpdater.OperationType.REMOVE;
               };
               ops.add(new ProtobufFieldUpdater.UpdateOperation(opType, uo.getPropertyPath(), uo.getValues()));
            }

            byte[] updatedInner = ProtobufFieldUpdater.update(descriptor, innerBytes, ops);

            byte[] updatedWrapped = rewrap(serCtx, typeName, typeId, updatedInner);
            storageCache.put(key, wrapBytes(updatedWrapped, storageCache));
            return 1;
         } catch (Exception e) {
            throw new RuntimeException("Failed to apply update for key: " + key, e);
         }
      }

      private byte[] extractBytes(Object value) {
         if (value instanceof byte[] b) return b;
         if (value instanceof org.infinispan.commons.marshall.WrappedByteArray w) return w.getBytes();
         throw new IllegalArgumentException("Cannot extract bytes from: " + value.getClass());
      }

      private Object wrapBytes(byte[] bytes, AdvancedCache<?, ?> cache) {
         DataConversion valueConversion = cache.getValueDataConversion();
         return valueConversion.toStorage(bytes);
      }

      private byte[] rewrap(ImmutableSerializationContext ctx, String typeName, Integer typeId, byte[] innerBytes) throws java.io.IOException {
         var baos = new org.infinispan.protostream.impl.RandomAccessOutputStreamImpl(innerBytes.length + 20);
         var writer = org.infinispan.protostream.impl.TagWriterImpl.newInstance(ctx, (org.infinispan.protostream.RandomAccessOutputStream) baos);

         if (typeId != null) {
            writer.writeUInt32(WrappedMessage.WRAPPED_TYPE_ID, typeId);
         } else if (typeName != null) {
            writer.writeString(WrappedMessage.WRAPPED_TYPE_NAME, typeName);
         }
         writer.writeBytes(WrappedMessage.WRAPPED_MESSAGE, innerBytes);
         writer.flush();
         return baos.toByteArray();
      }
   }

   @Override
   public String toString() {
      return "EmbeddedQuery{" +
            "queryString=" + queryString +
            ", statementType=" + statementType +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", defaultMaxResults=" + defaultMaxResults +
            ", timeout=" + timeout +
            '}';
   }
}
