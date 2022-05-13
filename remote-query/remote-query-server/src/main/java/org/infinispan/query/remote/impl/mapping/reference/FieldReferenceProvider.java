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

public class FieldReferenceProvider {

   private static final Log log = LogFactory.getLog(FieldReferenceProvider.class, Log.class);

   private final String name;
   private final Type type;
   private final boolean repeated;

   private final Searchable searchable;
   private final Projectable projectable;
   private final Aggregable aggregable;
   private final Sortable sortable;
   private final String analyzer;
   private final String normalizer;
   private final Object indexNullAs;

   public FieldReferenceProvider(FieldDescriptor fieldDescriptor, FieldMapping fieldMapping) {
      // the property name and type are taken from the model
      name = fieldDescriptor.getName();
      type = fieldDescriptor.getType();
      repeated = fieldDescriptor.isRepeated();

      searchable = (fieldMapping.searchable()) ? Searchable.YES : Searchable.NO;
      projectable = (fieldMapping.projectable()) ? Projectable.YES : Projectable.NO;
      aggregable = (fieldMapping.aggregable()) ? Aggregable.YES : Aggregable.NO;
      sortable = (fieldMapping.sortable()) ? Sortable.YES : Sortable.NO;
      analyzer = fieldMapping.analyzer();
      normalizer = fieldMapping.normalizer();
      indexNullAs = fieldMapping.parseIndexNullAs();
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
      if (normalizer != null) {
         step.normalizer(normalizer);
         return;
      }

      // TODO the following algorithm is used to support normalizer using legacy annotation,
      //      this won't be necessary when they are removed.
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
