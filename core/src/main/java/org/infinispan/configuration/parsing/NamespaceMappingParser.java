package org.infinispan.configuration.parsing;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationReaderException;

/**
 *
 * NamespaceMappingParser. This interface defines methods exposed by a namespace-mapping-aware
 * parser (such as {@link ParserRegistry})
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public interface NamespaceMappingParser {

   /**
    * Recursively parses the current element of an XML stream using an appropriate
    * {@link ConfigurationParser} depending on the element's namespace.
    *
    * @param reader the configuration stream reader
    * @param holder a configuration holder
    * @throws ConfigurationReaderException
    */
   void parseElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) throws ConfigurationReaderException;

   /**
    * Handle a namespaced attribute
    * @param reader the configuration stream reader
    * @param i the index of the attribute
    * @param holder a configuration holder
    * @throws ConfigurationReaderException
    */
   void parseAttribute(ConfigurationReader reader, int i, ConfigurationBuilderHolder holder) throws ConfigurationReaderException;
}
