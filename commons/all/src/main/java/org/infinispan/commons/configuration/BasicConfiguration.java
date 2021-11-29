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
   @Deprecated
   default String toXMLString(String name) {
      return toStringConfiguration(name);
   }

   /**
    * Converts this configuration to a string-based representation. The name of the configuration in the will be the one
    * supplied in the argument. The string must be in one of the supported formats (XML, JSON, YAML).
    *
    * @return a String containing the representation of an Infinispan configuration using the Infinispan schema in one of the supported formats (XML, JSON, YAML).
    */
   String toStringConfiguration(String name);
}
