package org.infinispan.query.functions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.infinispan.query.objectfilter.impl.syntax.update.QueryFunctionRegistry;

/**
 * Context for evaluating UPDATE statement value expressions: the materialized
 * target entity, a reader for its current property values, the named
 * parameters of the query and the registry of available functions.
 * <p>
 * Unlike the {@link org.infinispan.api.query.functions.QueryFunctionContext}
 * interface seen by user-defined functions, this class exposes the registry of
 * available functions, which is needed to resolve nested function calls but is
 * not part of the extension-point contract.
 *
 * @since 16.4
 */
public final class QueryFunctionContext implements org.infinispan.api.query.functions.QueryFunctionContext {

   private final Object entity;
   private final Function<String[], Object> propertyReader;
   private final Map<String, Object> namedParameters;
   private final QueryFunctionRegistry functions;
   private final Map<String, Pattern> patternCache;

   public QueryFunctionContext(Object entity,
                               Function<String[], Object> propertyReader,
                               Map<String, Object> namedParameters,
                               QueryFunctionRegistry functions,
                               Map<String, Pattern> patternCache) {
      this.entity = entity;
      this.propertyReader = propertyReader;
      this.namedParameters = namedParameters;
      this.functions = functions;
      this.patternCache = patternCache;
   }

   public QueryFunctionContext(Object entity,
                               Function<String[], Object> propertyReader,
                               Map<String, Object> namedParameters,
                               QueryFunctionRegistry functions) {
      this(entity, propertyReader, namedParameters, functions, new ConcurrentHashMap<>());
   }

   @Override
   public Object getEntity() {
      return entity;
   }

   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   public QueryFunctionRegistry getFunctions() {
      return functions;
   }

   /**
    * Returns a compiled {@link Pattern} for the given regex string, using a
    * shared cache so that the same pattern is compiled at most once across
    * all entity evaluations.
    *
    * @param regex the regular-expression string
    * @return the compiled pattern
    */
   @Override
   public Pattern compilePattern(String regex) {
      return patternCache.computeIfAbsent(regex, Pattern::compile);
   }

   /**
    * Reads the current value of the given property path from the target entity.
    *
    * @param path the property path, e.g. {@code ["address", "city"]}
    * @return the current value, possibly {@code null}
    */
   @Override
   public Object getPropertyValue(String[] path) {
      return propertyReader.apply(path);
   }
}
