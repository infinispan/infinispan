package org.infinispan.configuration.serializing;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public interface XMLExtendedStreamWriter extends XMLStreamWriter {
   void writeAttribute(String localName, String[] values) throws XMLStreamException;

   void writeAttribute(String prefix, String namespaceURI, String localName, String[] values) throws XMLStreamException;

   void writeAttribute(String namespaceURI, String localName, String[] values) throws XMLStreamException;

   void writeAttribute(String localName, Iterable<String> value) throws XMLStreamException;

   void writeAttribute(String prefix, String namespaceURI, String localName, Iterable<String> value) throws XMLStreamException;

   void writeAttribute(String namespaceURI, String localName, Iterable<String> value) throws XMLStreamException;

   void setUnspecifiedElementNamespace(String namespace);

   void writeStartElement(Enum<?> name) throws XMLStreamException;

   void writeAttribute(Enum<?> name, String property) throws XMLStreamException;

   void writeEmptyElement(Enum<?> name) throws XMLStreamException;
}
