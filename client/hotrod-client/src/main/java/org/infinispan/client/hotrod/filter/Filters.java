package org.infinispan.client.hotrod.filter;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQuery;

import java.util.Map;

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

   public static Object[] makeFactoryParams(Query query) {
      BaseQuery baseQuery = (BaseQuery) query;
      Map<String, Object> namedParameters = baseQuery.getNamedParameters();
      if (namedParameters == null) {
         return new Object[]{baseQuery.getJPAQuery()};
      }
      Object[] factoryParams = new Object[1 + namedParameters.size() * 2];
      factoryParams[0] = baseQuery.getJPAQuery();
      int i = 1;
      for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
         factoryParams[i++] = e.getKey();
         factoryParams[i++] = e.getValue();
      }
      return factoryParams;
   }

}
