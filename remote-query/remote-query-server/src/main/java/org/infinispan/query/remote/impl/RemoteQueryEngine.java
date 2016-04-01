package org.infinispan.query.remote.impl;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.builtin.BooleanBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.StringBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.bridge.builtin.impl.TwoWayString2FieldBridgeAdaptor;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.objectfilter.impl.BaseMatcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.embedded.impl.ResultProcessor;
import org.infinispan.query.dsl.embedded.impl.RowProcessor;
import org.infinispan.query.dsl.embedded.impl.jpalucene.HibernateSearchPropertyHelper;
import org.infinispan.query.dsl.embedded.impl.jpalucene.JPALuceneTransformer;
import org.infinispan.query.dsl.embedded.impl.jpalucene.LuceneQueryParsingResult;
import org.infinispan.query.remote.impl.filter.JPAProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
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

   private static final FieldBridge DOUBLE_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.DOUBLE_FIELD_BRIDGE, QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge FLOAT_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.FLOAT_FIELD_BRIDGE, QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge LONG_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.LONG_FIELD_BRIDGE, QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge INT_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.INT_FIELD_BRIDGE, QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge STRING_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge BOOL_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(new BooleanBridge()), QueryFacadeImpl.NULL_TOKEN_CODEC);

   private final boolean isCompatMode;

   private final SerializationContext serCtx;

   public RemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed, boolean isCompatMode, SerializationContext serCtx) {
      super(cache, isIndexed);
      this.isCompatMode = isCompatMode;
      this.serCtx = serCtx;
   }

   @Override
   protected BaseMatcher getMatcher() {
      Class<? extends BaseMatcher> matcherImplClass = isCompatMode ? CompatibilityReflectionMatcher.class : ProtobufMatcher.class;
      return SecurityActions.getCacheComponentRegistry(cache).getComponent(matcherImplClass);
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
      return isCompatMode ? query : new BooleanQuery.Builder()
            .add(new BooleanClause(new TermQuery(new Term(QueryFacadeImpl.TYPE_FIELD_NAME, targetEntityName)), BooleanClause.Occur.MUST))
            .add(new BooleanClause(query, BooleanClause.Occur.MUST))
            .build();
   }

   @Override
   protected JPAFilterAndConverter createFilter(String jpaQuery, Map<String, Object> namedParameters) {
      return isIndexed && !isCompatMode ? new JPAProtobufFilterAndConverter(jpaQuery, namedParameters) :
            new JPAFilterAndConverter(jpaQuery, namedParameters, isCompatMode ? CompatibilityReflectionMatcher.class : ProtobufMatcher.class);
   }

   @Override
   protected BooleShannonExpansion.IndexedFieldProvider getIndexedFieldProvider(FilterParsingResult<?> parsingResult) {
      return isCompatMode ? super.getIndexedFieldProvider(parsingResult) :
            new ProtobufIndexedFieldProvider((Descriptor) parsingResult.getTargetEntityMetadata());
   }

   @Override
   protected Class<?> getTargetedClass(LuceneQueryParsingResult<?> parsingResult) {
      return isCompatMode ? (Class<?>) parsingResult.getTargetEntityMetadata() : ProtobufValueWrapper.class;
   }

   protected LuceneQueryParsingResult<?> transform(FilterParsingResult<?> parsingResult, Map<String, Object> namedParameters) {
      if (isCompatMode) {
         EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
            @Override
            public Class<?> getClassFromName(String entityName) {
               return serCtx.canMarshall(entityName) ? serCtx.getMarshaller(entityName).getJavaClass() : null;
            }
         };

         HibernateSearchPropertyHelper propertyHelper = new HibernateSearchPropertyHelper(getSearchFactory(), entityNamesResolver, null);
         return JPALuceneTransformer.transform(parsingResult, getSearchFactory(), entityNamesResolver, propertyHelper::getDefaultFieldBridge, namedParameters);
      } else {
         EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
            @Override
            public Class<?> getClassFromName(String entityName) {
               return serCtx.canMarshall(entityName) ? ProtobufValueWrapper.class : null;
            }
         };

         JPALuceneTransformer.FieldBridgeProvider fieldBridgeProvider = new JPALuceneTransformer.FieldBridgeProvider() {
            @Override
            public FieldBridge getFieldBridge(String typeName, String[] propertyPath) {
               FieldDescriptor fd = getFieldDescriptor(serCtx, typeName, propertyPath);
               switch (fd.getType()) {
                  case DOUBLE:
                     return DOUBLE_FIELD_BRIDGE;
                  case FLOAT:
                     return FLOAT_FIELD_BRIDGE;
                  case INT64:
                  case UINT64:
                  case FIXED64:
                  case SFIXED64:
                  case SINT64:
                     return LONG_FIELD_BRIDGE;
                  case INT32:
                  case FIXED32:
                  case UINT32:
                  case SFIXED32:
                  case SINT32:
                  case ENUM:
                     return INT_FIELD_BRIDGE;
                  case BOOL:
                     return BOOL_FIELD_BRIDGE;
                  case STRING:
                  case BYTES:
                  case GROUP:
                  case MESSAGE:
                     return STRING_FIELD_BRIDGE;
               }
               return null;
            }
         };

         return JPALuceneTransformer.transform(parsingResult, getSearchFactory(), entityNamesResolver, fieldBridgeProvider, namedParameters);
      }
   }

   private FieldDescriptor getFieldDescriptor(SerializationContext serCtx, String type, String[] attributePath) {
      Descriptor messageDescriptor = serCtx.getMessageDescriptor(type);
      FieldDescriptor fd = null;
      for (int i = 0; i < attributePath.length; i++) {
         String name = attributePath[i];
         fd = messageDescriptor.findFieldByName(name);
         if (fd == null) {
            throw log.unknownField(name, messageDescriptor.getFullName());
         }
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null && !indexingMetadata.isFieldIndexed(fd.getNumber())) {
            throw log.fieldIsNotIndexed(name, messageDescriptor.getFullName());
         }
         if (i < attributePath.length - 1) {
            messageDescriptor = fd.getMessageType();
         }
      }
      return fd;
   }
}
