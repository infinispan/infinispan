package org.infinispan.objectfilter.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
abstract class ObjectFilterBase<TypeMetadata> implements ObjectFilter {

   private static final Log log = Logger.getMessageLogger(Log.class, ObjectFilterBase.class.getName());

   protected final IckleParsingResult<TypeMetadata> parsingResult;

   protected final Map<String, Object> namedParameters;

   protected ObjectFilterBase(IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters) {
      this.parsingResult = parsingResult;
      this.namedParameters = namedParameters != null ? Collections.unmodifiableMap(namedParameters) : null;
   }

   protected void validateParameters(Map<String, Object> namedParameters) {
      if (namedParameters == null) {
         throw log.getNamedParametersCannotBeNull();
      }
      for (String paramName : getParameterNames()) {
         if (namedParameters.get(paramName) == null) {
            throw new IllegalArgumentException("Query parameter '" + paramName + "' was not set");
         }
      }
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
