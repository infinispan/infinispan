package org.infinispan.commons.configuration;

/**
 * A simple wrapper for an XML configuration represented as a String.
 *
 * @author Tristan Tarrant
 * @since 9.2
 * @deprecated Use {@link StringConfiguration} instead
 */
@Deprecated
public class XMLStringConfiguration extends StringConfiguration {
   public XMLStringConfiguration(String xml) {
      super(xml);
   }
}
