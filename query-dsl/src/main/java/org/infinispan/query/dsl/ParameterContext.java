package org.infinispan.query.dsl;

import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public interface ParameterContext<Context extends ParameterContext> {

   Map<String, Object> getParameters();

   Context setParameter(String paramName, Object paramValue);

   Context setParameters(Map<String, Object> paramValues);
}
