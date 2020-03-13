package org.infinispan.commons.configuration;

/**
 * BasicConfiguration provides the basis for concrete configurations.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface BasicConfiguration {
   @Deprecated
   default String toXMLString() {
      return toXMLString("configuration");
   }
   /**
    * Converts this configuration to its XML representation. The name of the configuration in the XML will be the one
    * supplied in the argument.
    *
    * @return a String containing the XML representation of an Infinispan configuration using the Infinispan schema.
    */
   String toXMLString(String name);
}
