package org.infinispan.query.dsl;

import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public interface ParameterContext<Context extends ParameterContext> {

   /**
    * Returns the named parameters Map.
    *
    * @return the named parameters (unmodifiable) or {@code null} if the query does not have parameters
    */
   Map<String, Object> getParameters();

   /**
    * Sets the value of a named parameter.
    *
    * @param paramName  the parameters name (non-empty and not null)
    * @param paramValue a non-null value
    * @return itself
    */
   Context setParameter(String paramName, Object paramValue);

   /**
    * Sets multiple named parameters at once. Parameters names cannot be empty or {@code null}. Parameter values must
    * not be {@code null}.
    *
    * @param paramValues a Map of parameters
    * @return itself
    */
   Context setParameters(Map<String, Object> paramValues);
}
