package org.infinispan.query.remote.impl.mapping.reference;

import org.hibernate.search.backend.lucene.analysis.model.impl.LuceneAnalysisDefinitionRegistry;
import org.hibernate.search.backend.lucene.types.dsl.impl.LuceneIndexFieldTypeFactoryImpl;
import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaElement;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaFieldOptionsStep;
import org.hibernate.search.engine.backend.types.Aggregable;
import org.hibernate.search.engine.backend.types.Projectable;
import org.hibernate.search.engine.backend.types.Searchable;
import org.hibernate.search.engine.backend.types.Sortable;
import org.hibernate.search.engine.backend.types.dsl.IndexFieldTypeFactory;
import org.hibernate.search.engine.backend.types.dsl.IndexFieldTypeFinalStep;
import org.hibernate.search.engine.backend.types.dsl.StandardIndexFieldTypeOptionsStep;
import org.hibernate.search.engine.backend.types.dsl.StringIndexFieldTypeOptionsStep;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.search.mapper.mapping.impl.DefaultAnalysisConfigurer;

public class FieldReferenceProvider {

   private static final Log log = LogFactory.getLog(FieldReferenceProvider.class, Log.class);

   private final String name;
   private final Type type;

   private final Searchable searchable;
   private final Projectable projectable;
   private final Aggregable aggregable;
   private final Sortable sortable;

   private final String analyzer;
   private final Object indexNullAs;
   private final boolean repeated;

   public FieldReferenceProvider(FieldDescriptor fieldDescriptor, FieldMapping fieldMapping) {
      // the property name and type are taken from the model
      name = fieldDescriptor.getName();
      type = fieldDescriptor.getType();

      analyzer = getAnalyzer(fieldMapping);

      // the rest from the mapping
      searchable = (fieldMapping.index()) ? Searchable.YES : Searchable.NO;
      sortable = getSortable(fieldMapping);

      projectable = (fieldMapping.store()) ? Projectable.YES : Projectable.NO;
      // aggregation at the moment are implemented by ISPN
      aggregable = Aggregable.NO;

      indexNullAs = fieldMapping.parseIndexNullAs();

      repeated = fieldDescriptor.isRepeated();
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

   private String getAnalyzer(FieldMapping fieldMapping) {
      if (!Type.STRING.equals(type) || !fieldMapping.analyze()) {
         return null;
      }

      return (fieldMapping.analyzer() != null) ? fieldMapping.analyzer() :
            DefaultAnalysisConfigurer.STANDARD_ANALYZER_NAME;
   }

   private Sortable getSortable(FieldMapping fieldMapping) {
      if (analyzer != null) {
         return Sortable.NO;
      }

      return (fieldMapping.sortable() || fieldMapping.store()) ? Sortable.YES : Sortable.NO;
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
            bindAnalyzer(typeFactory, step);
            return step;
         }
         case BYTE_STRING:
            return typeFactory.asString();
         default:
            throw log.fieldTypeNotIndexable(type.toString(), name);
      }
   }

   private void bindAnalyzer(IndexFieldTypeFactory typeFactory, StringIndexFieldTypeOptionsStep<?> step) {
      if (analyzer == null) {
         return;
      }

      @SuppressWarnings("uncheked")
      LuceneAnalysisDefinitionRegistry analysisDefinitionRegistry =
            ((LuceneIndexFieldTypeFactoryImpl) typeFactory).getAnalysisDefinitionRegistry();

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
