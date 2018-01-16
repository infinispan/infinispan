package org.infinispan.commons.configuration;

/**
 * BasicConfiguration provides the basis for concrete configurations.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface BasicConfiguration {
   /**
    * Converts this configuration to its XML representation. The name of the configuration in the XML must be "configuration".
    *
    * @return a String containing the XML representation of an Infinispan configuration using the Infinispan schema.
    */
   String toXMLString();
}
