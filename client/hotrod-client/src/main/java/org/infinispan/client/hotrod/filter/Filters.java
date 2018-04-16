package org.infinispan.client.hotrod.filter;

import java.util.Map;

import org.infinispan.query.dsl.Query;

/**
 * @author gustavonalle
 * @since 8.1
 */
public final class Filters {

   /**
    * The name of the factory used for query DSL based filters and converters. This factory is provided internally by
    * the server.
    */
   public static final String QUERY_DSL_FILTER_FACTORY_NAME = "query-dsl-filter-converter-factory";

   public static final String ITERATION_QUERY_FILTER_CONVERTER_FACTORY_NAME = "iteration-filter-converter-factory";

   public static final String CONTINUOUS_QUERY_FILTER_FACTORY_NAME = "continuous-query-filter-converter-factory";

   private Filters() {
   }

   public static Object[] makeFactoryParams(String queryString, Map<String, Object> namedParameters) {
      if (namedParameters == null) {
         return new Object[]{queryString};
      }
      Object[] factoryParams = new Object[1 + namedParameters.size() * 2];
      factoryParams[0] = queryString;
      int i = 1;
      for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
         factoryParams[i++] = e.getKey();
         factoryParams[i++] = e.getValue();
      }
      return factoryParams;
   }

   public static Object[] makeFactoryParams(Query query) {
      return makeFactoryParams(query.getQueryString(), query.getParameters());
   }
}
