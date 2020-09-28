package org.infinispan.search.mapper.mapping;

import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.ProgrammaticMappingConfigurationContext;

public interface MappingConfigurationContext {

   /**
    * Start the definition of a programmatic mapping.
    * @return A context to define the programmatic mapping.
    */
   ProgrammaticMappingConfigurationContext programmaticMapping();

}
