package org.infinispan.commons.configuration;

import org.infinispan.commons.dataconversion.MediaType;

/**
 * BasicConfiguration provides the basis for concrete configurations.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface BasicConfiguration {
   @Deprecated(forRemoval = true)
   default String toXMLString() {
      return toXMLString("configuration");
   }
   /**
    * Converts this configuration to its XML representation. The name of the configuration in the XML will be the one
    * supplied in the argument.
    *
    * @return a String containing the XML representation of an Infinispan configuration using the Infinispan schema.
    */
   @Deprecated(forRemoval = true)
   default String toXMLString(String name) {
      return toStringConfiguration(name);
   }

   /**
    * Converts this configuration to an XML.
    *
    * @param name The name of the configuration in the generated string.
    *
    * @return a String containing the representation of an Infinispan configuration using the Infinispan schema in XML.
    */
   default String toStringConfiguration(String name) {
      return toStringConfiguration(name, MediaType.APPLICATION_XML, true);
   }

   /**
    * Converts this configuration to a string representation.
    *
    * @param name The name of the configuration in the generated string.
    * @param mediaType The type of string to generate. Can be one of XML, JSON or YAML.
    * @param clearTextSecrets Whether secrets (e.g. passwords) should be included in clear text or masked.
    *
    * @return a String containing the representation of an Infinispan configuration using the Infinispan schema in one of the supported formats (XML, JSON, YAML).
    */
   String toStringConfiguration(String name, MediaType mediaType, boolean clearTextSecrets);
}
