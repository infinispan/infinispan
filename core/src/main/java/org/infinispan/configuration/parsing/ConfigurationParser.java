package org.infinispan.configuration.parsing;

import javax.xml.stream.XMLStreamException;


/**
 *
 * ConfigurationParser.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConfigurationParser {

   /**
    * The entry point of a configuration parser which gets passed a {@link XMLExtendedStreamReader} positioned at a root
    * element associated with the parser itself according to the registered mapping.
    *
    * @param reader the XML stream reader
    * @param holder a holder object used by the parser to maintain state
    * @throws XMLStreamException
    */
   void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException;

   Namespace[] getNamespaces();
}
