package org.infinispan.query.remote.impl;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.LuceneQueryMaker;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.embedded.impl.ResultProcessor;
import org.infinispan.query.dsl.embedded.impl.RowProcessor;
import org.infinispan.query.remote.impl.filter.JPAProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.impl.logging.Log;

import java.util.Arrays;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
final class RemoteQueryEngine extends QueryEngine {

   private static final Log log = LogFactory.getLog(RemoteQueryEngine.class, Log.class);

   private final boolean isCompatMode;

   private final ProtobufFieldBridgeProvider protobufFieldBridgeProvider;

   public RemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed, boolean isCompatMode, SerializationContext serCtx) {
      super(cache, isIndexed, isCompatMode ? CompatibilityReflectionMatcher.class : ProtobufMatcher.class);
      this.isCompatMode = isCompatMode;
      protobufFieldBridgeProvider = new ProtobufFieldBridgeProvider(serCtx);
   }

   @Override
   protected ResultProcessor makeResultProcessor(ResultProcessor in) {
      return result -> {
         if (result instanceof ProtobufValueWrapper) {
            result = ((ProtobufValueWrapper) result).getBinary();
         }
         return in != null ? in.process(result) : result;
      };
   }

   @Override
   protected RowProcessor makeProjectionProcessor(Class<?>[] projectedTypes) {
      if (isCompatMode) {
         return null;
      }

      // Protobuf's booleans are indexed as Strings, so we need to convert them.
      // Collect here the positions of all Boolean projections.
      int[] pos = new int[projectedTypes.length];
      int len = 0;
      for (int i = 0; i < projectedTypes.length; i++) {
         if (projectedTypes[i] == Boolean.class) {
            pos[len++] = i;
         }
      }
      if (len == 0) {
         return null;
      }
      final int[] cols = len < pos.length ? Arrays.copyOf(pos, len) : pos;
      return row -> {
         for (int i : cols) {
            if (row[i] != null) {
               // the Boolean column is actually encoded as a String, so we convert it
               row[i] = "true".equals(row[i]);
            }
         }
         return row;
      };
   }

   @Override
   protected org.apache.lucene.search.Query makeTypeQuery(org.apache.lucene.search.Query query, String targetEntityName) {
      return isCompatMode ? query :
            new BooleanQuery.Builder()
                  .add(new BooleanClause(new TermQuery(new Term(QueryFacadeImpl.TYPE_FIELD_NAME, targetEntityName)), BooleanClause.Occur.MUST))
                  .add(new BooleanClause(query, BooleanClause.Occur.MUST))
                  .build();
   }

   @Override
   protected JPAFilterAndConverter createFilter(String jpaQuery, Map<String, Object> namedParameters) {
      return isIndexed && !isCompatMode ? new JPAProtobufFilterAndConverter(jpaQuery, namedParameters) :
            new JPAFilterAndConverter(jpaQuery, namedParameters, matcher.getClass());
   }

   @Override
   protected BooleShannonExpansion.IndexedFieldProvider getIndexedFieldProvider(FilterParsingResult<?> parsingResult) {
      return isCompatMode ? super.getIndexedFieldProvider(parsingResult) :
            new ProtobufIndexedFieldProvider((Descriptor) parsingResult.getTargetEntityMetadata());
   }

   @Override
   protected Class<?> getTargetedClass(FilterParsingResult<?> parsingResult) {
      return isCompatMode ? (Class<?>) parsingResult.getTargetEntityMetadata() : ProtobufValueWrapper.class;
   }

   @Override
   protected LuceneQueryMaker createLuceneMaker() {
      return isCompatMode ? super.createLuceneMaker() : new LuceneQueryMaker(getSearchFactory(), null, protobufFieldBridgeProvider);
   }
}
