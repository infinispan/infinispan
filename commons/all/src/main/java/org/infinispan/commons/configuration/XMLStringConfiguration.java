package org.infinispan.commons.configuration;

/**
 * A simple wrapper for an XML configuration represented as a String.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class XMLStringConfiguration implements BasicConfiguration {
   private final String xml;

   public XMLStringConfiguration(String xml) {
      this.xml = xml;
   }

   @Override
   public String toXMLString(String name) {
      return xml;
   }
}
