package org.infinispan.commons.configuration;

/**
 * A simple wrapper for an XML configuration represented as a String.
 *
 * @author Tristan Tarrant
 * @since 9.2
 * @deprecated Use {@link StringConfiguration} instead
 */
@Deprecated(forRemoval=true, since = "14.0")
public class XMLStringConfiguration extends StringConfiguration {
   public XMLStringConfiguration(String xml) {
      super(xml);
   }
}
