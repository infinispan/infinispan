package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
abstract class ObjectFilterBase<TypeMetadata> implements ObjectFilter {

   protected final FilterParsingResult<TypeMetadata> parsingResult;

   protected final Map<String, Object> namedParameters;

   protected ObjectFilterBase(FilterParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters) {
      this.parsingResult = parsingResult;
      this.namedParameters = namedParameters != null ? Collections.unmodifiableMap(namedParameters) : null;
   }

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
   public Set<String> getParameterNames() {
      return parsingResult.getParameterNames();
   }

   @Override
   public Map<String, Object> getParameters() {
      return namedParameters;
   }
}
