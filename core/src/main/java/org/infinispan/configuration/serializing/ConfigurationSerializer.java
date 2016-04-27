package org.infinispan.configuration.serializing;

import javax.xml.stream.XMLStreamException;

/**
 * @author Tristan Tarrant
 * @since 9.0
 */
public interface ConfigurationSerializer<T> {
   void serialize(XMLExtendedStreamWriter writer, T configuration) throws XMLStreamException;
}
