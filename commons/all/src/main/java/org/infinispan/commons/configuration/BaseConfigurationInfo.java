package org.infinispan.commons.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Defines base methods for exposing information about a configuration attributes and elements.
 */
public interface BaseConfigurationInfo {

   /**
    * @return the {@link AttributeSet} declared by the configuration.
    */
   default AttributeSet attributes() {
      return null;
   }

   /**
    * @return the {@link ElementDefinition} of the configuration.
    */
   default ElementDefinition getElementDefinition() {
      return null;
   }

}
