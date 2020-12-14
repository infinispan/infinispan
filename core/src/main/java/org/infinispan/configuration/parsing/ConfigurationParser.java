package org.infinispan.configuration.parsing;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
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

   /**
    * @return The {@link ConfigurationBuilderInfo} used to build the root element of the parser.
    */
   default Class<? extends ConfigurationBuilderInfo> getConfigurationBuilderInfo() {
      return null;
   }

   default void readAttribute(ConfigurationReader reader, String name, String attributeName, String attributeValue, ConfigurationBuilderHolder holder) {
      throw new UnsupportedOperationException("This parser cannot handle namespaced attributes");
   }
}
