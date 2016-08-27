package org.infinispan.configuration.parsing;

import javax.xml.stream.XMLStreamException;

/**
 *
 * NamespaceMappingParser. This interface defines methods exposed by a namespace-mapping-aware
 * parser (such as {@link ParserRegistry}
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public interface NamespaceMappingParser {

   /**
    * Recursively parses the current element of an XML stream using an appropriate
    * {@link ConfigurationParser} depending on the element's namespace.
    *
    * @param reader the XML stream reader
    * @param holder a configuration holder
    * @throws XMLStreamException
    */
   void parseElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException;

}
