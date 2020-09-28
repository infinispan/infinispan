package org.infinispan.search.mapper.mapping;

/**
 * An object responsible for configuring the Hibernate Search mapping.
 */
public interface ProgrammaticSearchMappingProvider {

   /**
    * Configure the Hibernate Search mapping as necessary using the given {@code context}.
    *
    * @param context A context exposing methods to configure the mapping.
    */
   void configure(MappingConfigurationContext context);

}
