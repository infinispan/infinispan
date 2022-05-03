package org.infinispan.hotrod.filter;

/**
 * @since 14.0
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
}
