package org.infinispan.search.mapper.mapping.metamodel;

import java.util.Optional;

import org.hibernate.search.engine.backend.metamodel.IndexValueFieldTypeDescriptor;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public class ValueFieldMetamodel implements JsonSerialization {

   private final boolean multiValued;
   private final boolean multiValuedInRoot;

   private final Class<?> type;
   private final Class<?> projectionType;
   private final Class<?> argumentType;

   private final boolean searchable;
   private final boolean projectable;
   private final boolean sortable;
   private final boolean aggregable;

   private final Optional<String> analyzer;
   private final Optional<String> normalizer;

   public ValueFieldMetamodel(boolean multiValued, boolean multiValuedInRoot, IndexValueFieldTypeDescriptor type) {
      this.multiValued = multiValued;
      this.multiValuedInRoot = multiValuedInRoot;

      this.type = type.valueClass();
      projectionType = type.projectedValueClass();
      argumentType = type.dslArgumentClass();

      searchable = type.searchable();
      sortable = type.sortable();
      projectable = type.projectable();
      aggregable = type.aggregable();

      analyzer = type.analyzerName();
      normalizer = type.normalizerName();
   }

   public boolean isMultiValued() {
      return multiValued;
   }

   public boolean isMultiValuedInRoot() {
      return multiValuedInRoot;
   }

   public Class<?> getType() {
      return type;
   }

   public Class<?> getProjectionType() {
      return projectionType;
   }

   public Class<?> getArgumentType() {
      return argumentType;
   }

   public boolean isSearchable() {
      return searchable;
   }

   public boolean isProjectable() {
      return projectable;
   }

   public boolean isSortable() {
      return sortable;
   }

   public boolean isAggregable() {
      return aggregable;
   }

   public Optional<String> getAnalyzer() {
      return analyzer;
   }

   public Optional<String> getNormalizer() {
      return normalizer;
   }

   @Override
   public Json toJson() {
      Json object = Json.object("multi-valued", multiValued, "multi-valued-in-root", multiValuedInRoot);
      object.set("type", type);
      object.set("projection-type", projectionType);
      object.set("argument-type", argumentType);
      object.set("searchable", searchable);
      object.set("sortable", sortable);
      object.set("projectable", projectable);
      object.set("aggregable", aggregable);

      if (analyzer.isPresent()) {
         object.set("analyzer", analyzer.get());
      }
      if (normalizer.isPresent()) {
         object.set("normalizer", normalizer.get());
      }

      return object;
   }
}
