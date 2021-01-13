package org.infinispan.configuration.parsing;

import org.infinispan.commons.configuration.io.ConfigurationReader;

/**
 *
 * ConfigurationParser.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConfigurationParser {

   /**
    * The entry point of a configuration parser which gets passed a {@link ConfigurationReader} positioned at a root
    * element associated with the parser itself according to the registered mapping.
    *
    * @param reader the configuration stream reader
    * @param holder a holder object used by the parser to maintain state
    */
   void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder);

   Namespace[] getNamespaces();

   default void readAttribute(ConfigurationReader reader, String elementName, int attributeIndex, ConfigurationBuilderHolder holder) {
      throw new UnsupportedOperationException("This parser cannot handle namespaced attributes");
   }
}
