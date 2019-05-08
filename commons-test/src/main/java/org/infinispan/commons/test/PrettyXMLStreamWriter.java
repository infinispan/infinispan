package org.infinispan.commons.test;

import java.util.Arrays;
import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * XMLStreamWriter that adds an indent for each open element.
 *
 * @author Dan Berindei
 * @since 10.0
 */
class PrettyXMLStreamWriter implements XMLStreamWriter {
   private static final int INDENT_STEP = 2;
   private static final int MAX_INDENT = 64;
   private static final char[] INDENT = new char[MAX_INDENT + 1];

   static {
      Arrays.fill(INDENT, ' ');
      INDENT[0] = '\n';
   }

   private XMLStreamWriter writer;
   private int indent;
   private boolean skipIndent;


   PrettyXMLStreamWriter(XMLStreamWriter writer) {
      this.writer = writer;
   }

   @Override
   public void writeStartElement(String localName) throws XMLStreamException {
      writeIndent(1);
      writer.writeStartElement(localName);
   }

   @Override
   public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException {
      writeIndent(1);
      writer.writeStartElement(namespaceURI, localName);
   }

   @Override
   public void writeStartElement(String prefix, String localName, String namespaceURI) throws XMLStreamException {
      writeIndent(1);
      writer.writeStartElement(prefix, localName, namespaceURI);
   }

   @Override
   public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException {
      writeIndent(0);
      writer.writeEmptyElement(namespaceURI, localName);
   }

   @Override
   public void writeEmptyElement(String prefix, String localName, String namespaceURI) throws XMLStreamException {
      writeIndent(0);
      writer.writeEmptyElement(prefix, localName, namespaceURI);
   }

   @Override
   public void writeEmptyElement(String localName) throws XMLStreamException {
      writeIndent(0);
      writer.writeEmptyElement(localName);
   }

   @Override
   public void writeEndElement() throws XMLStreamException {
      writeIndent(-1);
      writer.writeEndElement();
   }

   @Override
   public void writeEndDocument() throws XMLStreamException {
      writer.writeEndDocument();
   }

   @Override
   public void close() throws XMLStreamException {
      writer.close();
   }

   @Override
   public void flush() throws XMLStreamException {
      writer.flush();
   }

   @Override
   public void writeAttribute(String localName, String value) throws XMLStreamException {
      writer.writeAttribute(localName, value);
   }

   @Override
   public void writeAttribute(String prefix, String namespaceURI, String localName, String value)
      throws XMLStreamException {
      writer.writeAttribute(prefix, namespaceURI, localName, value);
   }

   @Override
   public void writeAttribute(String namespaceURI, String localName, String value) throws XMLStreamException {
      writer.writeAttribute(namespaceURI, localName, value);
   }

   @Override
   public void writeNamespace(String prefix, String namespaceURI) throws XMLStreamException {
      writer.writeNamespace(prefix, namespaceURI);
   }

   @Override
   public void writeDefaultNamespace(String namespaceURI) throws XMLStreamException {
      writer.writeDefaultNamespace(namespaceURI);
   }

   @Override
   public void writeComment(String data) throws XMLStreamException {
      writeIndent(0);
      writer.writeComment(data);
   }

   @Override
   public void writeProcessingInstruction(String target) throws XMLStreamException {
      writer.writeProcessingInstruction(target);
   }

   @Override
   public void writeProcessingInstruction(String target, String data) throws XMLStreamException {
      writer.writeProcessingInstruction(target, data);
   }

   @Override
   public void writeCData(String data) throws XMLStreamException {
      writeIndent(0);
      writer.writeCData(data);
   }

   @Override
   public void writeDTD(String dtd) throws XMLStreamException {
      writer.writeDTD(dtd);
   }

   @Override
   public void writeEntityRef(String name) throws XMLStreamException {
      writer.writeEntityRef(name);
   }

   @Override
   public void writeStartDocument() throws XMLStreamException {
      writer.writeStartDocument();
   }

   @Override
   public void writeStartDocument(String version) throws XMLStreamException {
      writer.writeStartDocument(version);
   }

   @Override
   public void writeStartDocument(String encoding, String version) throws XMLStreamException {
      writer.writeStartDocument(encoding, version);
   }

   @Override
   public void writeCharacters(String text) throws XMLStreamException {
      skipIndent = true;
      writer.writeCharacters(text);
   }

   @Override
   public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
      skipIndent = true;
      writer.writeCharacters(text, start, len);
   }

   @Override
   public String getPrefix(String uri) throws XMLStreamException {
      return writer.getPrefix(uri);
   }

   @Override
   public void setPrefix(String prefix, String uri) throws XMLStreamException {
      writer.setPrefix(prefix, uri);
   }

   @Override
   public void setDefaultNamespace(String uri) throws XMLStreamException {
      writer.setDefaultNamespace(uri);
   }

   @Override
   public void setNamespaceContext(NamespaceContext context) throws XMLStreamException {
      writer.setNamespaceContext(context);
   }

   @Override
   public NamespaceContext getNamespaceContext() {
      return writer.getNamespaceContext();
   }

   @Override
   public Object getProperty(String name) throws IllegalArgumentException {
      return writer.getProperty(name);
   }

   private void writeIndent(int increment) throws XMLStreamException {
      // Unindent before start element
      if (increment < 0) {
         indent += increment * INDENT_STEP;
      }

      if (skipIndent) {
         skipIndent = false;
      } else {
         writer.writeCharacters(INDENT, 0, Math.min(indent, MAX_INDENT) + 1);
      }

      // Indent after start element
      if (increment > 0) {
         indent += increment * INDENT_STEP;
      }
   }
}
