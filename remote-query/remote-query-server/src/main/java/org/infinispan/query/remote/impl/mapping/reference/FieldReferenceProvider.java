package org.infinispan.query.remote.impl.mapping.reference;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.hibernate.search.backend.lucene.analysis.model.impl.LuceneAnalysisDefinitionRegistry;
import org.hibernate.search.backend.lucene.types.dsl.impl.LuceneIndexFieldTypeFactoryImpl;
import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaElement;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaFieldOptionsStep;
import org.hibernate.search.engine.backend.types.Aggregable;
import org.hibernate.search.engine.backend.types.Norms;
import org.hibernate.search.engine.backend.types.Projectable;
import org.hibernate.search.engine.backend.types.Searchable;
import org.hibernate.search.engine.backend.types.Sortable;
import org.hibernate.search.engine.backend.types.TermVector;
import org.hibernate.search.engine.backend.types.dsl.IndexFieldTypeFactory;
import org.hibernate.search.engine.backend.types.dsl.IndexFieldTypeFinalStep;
import org.hibernate.search.engine.backend.types.dsl.ScaledNumberIndexFieldTypeOptionsStep;
import org.hibernate.search.engine.backend.types.dsl.StandardIndexFieldTypeOptionsStep;
import org.hibernate.search.engine.backend.types.dsl.StringIndexFieldTypeOptionsStep;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.logging.Log;

public class FieldReferenceProvider {

   private static final Log log = LogFactory.getLog(FieldReferenceProvider.class, Log.class);

   public static final String INFINISPAN_COMMON_TYPES_GROUP = "org.infinispan.protostream.commons.";

   public static final String BIG_INTEGER_COMMON_TYPE = INFINISPAN_COMMON_TYPES_GROUP + "BigInteger";
   public static final String BIG_DECIMAL_COMMON_TYPE = INFINISPAN_COMMON_TYPES_GROUP + "BigDecimal";

   static final String[] COMMON_MESSAGE_TYPES = {BIG_INTEGER_COMMON_TYPE, BIG_DECIMAL_COMMON_TYPE};

   private final String name;
   private final Type type;
   private final String typeName;
   private final boolean repeated;

   private final Searchable searchable;
   private final Projectable projectable;
   private final Aggregable aggregable;
   private final Sortable sortable;
   private final String analyzer;
   private final String normalizer;
   private final Object indexNullAs;

   private final Norms norms;
   private final String searchAnalyzer;
   private final TermVector termVector;
   private final Integer decimalScale;

   public FieldReferenceProvider(FieldDescriptor fieldDescriptor, FieldMapping fieldMapping) {
      // the property name and type are taken from the model
      name = fieldDescriptor.getName();
      type = fieldDescriptor.getType();
      typeName = fieldDescriptor.getTypeName();
      repeated = fieldDescriptor.isRepeated();

      searchable = (fieldMapping.searchable()) ? Searchable.YES : Searchable.NO;
      projectable = (fieldMapping.projectable()) ? Projectable.YES : Projectable.NO;
      aggregable = (fieldMapping.aggregable()) ? Aggregable.YES : Aggregable.NO;
      sortable = (fieldMapping.sortable()) ? Sortable.YES : Sortable.NO;
      analyzer = fieldMapping.analyzer();
      normalizer = fieldMapping.normalizer();
      indexNullAs = fieldMapping.parseIndexNullAs();
      norms = (fieldMapping.norms() == null) ? null : (fieldMapping.norms()) ? Norms.YES : Norms.NO;
      searchAnalyzer = fieldMapping.searchAnalyzer();
      termVector = termVector(fieldMapping.termVector());
      decimalScale = fieldMapping.decimalScale();
   }

   private static TermVector termVector(org.infinispan.api.annotations.indexing.option.TermVector termVector) {
      if (termVector == null) {
         return null;
      }

      switch (termVector) {
         case YES:
            return TermVector.YES;
         case NO:
            return TermVector.NO;
         case WITH_POSITIONS:
            return TermVector.WITH_POSITIONS;
         case WITH_OFFSETS:
            return TermVector.WITH_OFFSETS;
         case WITH_POSITIONS_OFFSETS:
            return TermVector.WITH_POSITIONS_OFFSETS;
         case WITH_POSITIONS_PAYLOADS:
            return TermVector.WITH_POSITIONS_PAYLOADS;
         case WITH_POSITIONS_OFFSETS_PAYLOADS:
            return TermVector.WITH_POSITIONS_OFFSETS_PAYLOADS;
      }
      return null;
   }

   public String getName() {
      return name;
   }

   public IndexFieldReference<Object> bind(IndexSchemaElement indexSchemaElement) {
      if (nothingToBind()) {
         return null;
      }

      IndexSchemaFieldOptionsStep<?, IndexFieldReference<Object>> step = indexSchemaElement.field(name, this::bind);
      if (repeated) {
         step.multiValued();
      }
      return step.toReference();
   }

   public boolean nothingToBind() {
      return Searchable.NO.equals(searchable) && Projectable.NO.equals(projectable) &&
            Aggregable.NO.equals(aggregable) && Sortable.NO.equals(sortable);
   }

   private <F> IndexFieldTypeFinalStep<F> bind(IndexFieldTypeFactory typeFactory) {
      StandardIndexFieldTypeOptionsStep<?, F> optionsStep = (StandardIndexFieldTypeOptionsStep<?, F>) bindType(typeFactory);
      optionsStep.searchable(searchable).sortable(sortable).projectable(projectable).aggregable(aggregable);
      if (indexNullAs != null) {
         optionsStep.indexNullAs((F) indexNullAs);
      }

      return optionsStep;
   }

   private StandardIndexFieldTypeOptionsStep<?, ?> bindType(IndexFieldTypeFactory typeFactory) {
      switch (type.getJavaType()) {
         case ENUM:
         case INT:
            return typeFactory.asInteger();
         case LONG:
            return typeFactory.asLong();
         case FLOAT:
            return typeFactory.asFloat();
         case DOUBLE:
            return typeFactory.asDouble();
         case BOOLEAN:
            return typeFactory.asBoolean();
         case STRING: {
            StringIndexFieldTypeOptionsStep<?> step = typeFactory.asString();
            bindStringTypeOptions(typeFactory, step);
            return step;
         }
         case BYTE_STRING:
            return typeFactory.asString();
         case MESSAGE:
            return bindCommonMessageType(typeFactory);
         default:
            throw log.fieldTypeNotIndexable(type.toString(), name);
      }
   }

   private StandardIndexFieldTypeOptionsStep<?, ?> bindCommonMessageType(IndexFieldTypeFactory typeFactory) {
      if (BIG_INTEGER_COMMON_TYPE.equals(typeName)) {
         ScaledNumberIndexFieldTypeOptionsStep<?, BigInteger> step = typeFactory.asBigInteger();
         bindScaledNumberTypeOptions(step);
         return step;
      }
      if (BIG_DECIMAL_COMMON_TYPE.equals(typeName)) {
         ScaledNumberIndexFieldTypeOptionsStep<?, BigDecimal> step = typeFactory.asBigDecimal();
         bindScaledNumberTypeOptions(step);
         return step;
      }
      throw log.fieldTypeNotIndexable(typeName, name);
   }

   private void bindScaledNumberTypeOptions(ScaledNumberIndexFieldTypeOptionsStep<?, ?> step) {
      if (decimalScale != null) {
         step.decimalScale(decimalScale);
      } else {
         step.decimalScale(0); // if a basic is used
      }
   }

   private void bindStringTypeOptions(IndexFieldTypeFactory typeFactory, StringIndexFieldTypeOptionsStep<?> step) {
      bindNormalizerOrAnalyzer((LuceneIndexFieldTypeFactoryImpl) typeFactory, step);

      if (norms != null) {
         step.norms(norms);
      }
      if (searchAnalyzer != null) {
         step.searchAnalyzer(searchAnalyzer);
      }
      if (termVector != null) {
         step.termVector(termVector);
      }
   }

   private void bindNormalizerOrAnalyzer(LuceneIndexFieldTypeFactoryImpl typeFactory, StringIndexFieldTypeOptionsStep<?> step) {
      if (normalizer != null) {
         step.normalizer(normalizer);
         return;
      }

      // TODO the following algorithm is used to support normalizer using legacy annotation,
      //      this won't be necessary when they are removed.
      if (analyzer == null) {
         return;
      }

      LuceneAnalysisDefinitionRegistry analysisDefinitionRegistry = typeFactory.getAnalysisDefinitionRegistry();
      if (analysisDefinitionRegistry.getNormalizerDefinition(analyzer) != null) {
         step.normalizer(analyzer);
      } else {
         step.analyzer(analyzer);
      }
   }

   @Override
   public String toString() {
      return "{" +
            "name='" + name + '\'' +
            ", type=" + type +
            '}';
   }
}
