package org.infinispan.configuration.parsing;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @author Tristan Tarrant
 * @since 6.0
 */
final class XMLExtendedStreamReaderImpl implements XMLExtendedStreamReader {

   private final NamespaceMappingParser parser;
   private final XMLStreamReader streamReader;
   private final Deque<Context> stack = new ArrayDeque<Context>();
   private Schema schema;
   private Properties properties;

   XMLExtendedStreamReaderImpl(final NamespaceMappingParser parser, final XMLStreamReader streamReader, Properties properties) {
      this.parser = parser;
      this.streamReader = streamReader;
      this.properties = properties;
      stack.push(new Context());
   }

   @Override
   public void handleAny(final ConfigurationBuilderHolder holder) throws XMLStreamException {
      require(START_ELEMENT, null, null);
      boolean ok = false;
      try {
         final Deque<Context> stack = this.stack;
         stack.push(new Context());
         try {
            parser.parseElement(this, holder);
         } finally {
            stack.pop();
         }
         ok = true;
      } finally {
         if (!ok) {
            safeClose();
         }
      }
   }

   @Override
   public Object getProperty(final String name) throws IllegalArgumentException {
      if (properties.containsKey(name)) {
         return properties.getProperty(name);
      } else {
         return streamReader.getProperty(name);
      }
   }

   @Override
   public int next() throws XMLStreamException {
      final Context context = stack.getFirst();
      if (context.depth > 0) {
         final int next = streamReader.next();
         if (next == END_ELEMENT) {
            context.depth--;
         } else if (next == START_ELEMENT) {
            context.depth++;
         }
         return next;
      } else {
         try {
            throw readPastEnd(getLocation());
         } finally {
            safeClose();
         }
      }
   }

   @Override
   public void require(final int type, final String namespaceURI, final String localName) throws XMLStreamException {
      streamReader.require(type, namespaceURI, localName);
   }

   @Override
   public String getElementText() throws XMLStreamException {
      String text = streamReader.getElementText().trim();
      return replaceProperties(text, properties);
   }

   @Override
   public int nextTag() throws XMLStreamException {
      final Context context = stack.getFirst();
      if (context.depth > 0) {
         final int next = streamReader.nextTag();
         if (next == END_ELEMENT) {
            context.depth--;
         } else if (next == START_ELEMENT) {
            context.depth++;
         }
         return next;
      } else {
         try {
            throw readPastEnd(getLocation());
         } finally {
            safeClose();
         }
      }
   }

   @Override
   public boolean hasNext() throws XMLStreamException {
      return stack.getFirst().depth > 0 && streamReader.hasNext();
   }

   @Override
   public void close() throws XMLStreamException {
      throw new UnsupportedOperationException();
   }

   @Override
   public String getNamespaceURI(final String prefix) {
      return streamReader.getNamespaceURI(prefix);
   }

   @Override
   public boolean isStartElement() {
      return streamReader.isStartElement();
   }

   @Override
   public boolean isEndElement() {
      return streamReader.isEndElement();
   }

   @Override
   public boolean isCharacters() {
      return streamReader.isCharacters();
   }

   @Override
   public boolean isWhiteSpace() {
      return streamReader.isWhiteSpace();
   }

   @Override
   public String getAttributeValue(final String namespaceURI, final String localName) {
      String value = streamReader.getAttributeValue(namespaceURI, localName);
      return replaceProperties(value, properties);
   }

   @Override
   public int getAttributeCount() {
      return streamReader.getAttributeCount();
   }

   @Override
   public QName getAttributeName(final int index) {
      return streamReader.getAttributeName(index);
   }

   @Override
   public String getAttributeNamespace(final int index) {
      return streamReader.getAttributeNamespace(index);
   }

   @Override
   public String getAttributeLocalName(final int index) {
      return streamReader.getAttributeLocalName(index);
   }

   @Override
   public String getAttributePrefix(final int index) {
      return streamReader.getAttributePrefix(index);
   }

   @Override
   public String getAttributeType(final int index) {
      return streamReader.getAttributeType(index);
   }

   @Override
   public String getAttributeValue(final int index) {
      String value = streamReader.getAttributeValue(index);
      return replaceProperties(value, properties);
   }

   @Override
   public boolean isAttributeSpecified(final int index) {
      return streamReader.isAttributeSpecified(index);
   }

   @Override
   public int getNamespaceCount() {
      return streamReader.getNamespaceCount();
   }

   @Override
   public String getNamespacePrefix(final int index) {
      return streamReader.getNamespacePrefix(index);
   }

   @Override
   public String getNamespaceURI(final int index) {
      return streamReader.getNamespaceURI(index);
   }

   @Override
   public NamespaceContext getNamespaceContext() {
      return streamReader.getNamespaceContext();
   }

   @Override
   public int getEventType() {
      return streamReader.getEventType();
   }

   @Override
   public String getText() {
      return streamReader.getText();
   }

   @Override
   public char[] getTextCharacters() {
      return streamReader.getTextCharacters();
   }

   @Override
   public int getTextCharacters(final int sourceStart, final char[] target, final int targetStart, final int length) throws XMLStreamException {
      return streamReader.getTextCharacters(sourceStart, target, targetStart, length);
   }

   @Override
   public int getTextStart() {
      return streamReader.getTextStart();
   }

   @Override
   public int getTextLength() {
      return streamReader.getTextLength();
   }

   @Override
   public String getEncoding() {
      return streamReader.getEncoding();
   }

   @Override
   public boolean hasText() {
      return streamReader.hasText();
   }

   @Override
   public Location getLocation() {
      return streamReader.getLocation();
   }

   @Override
   public QName getName() {
      return streamReader.getName();
   }

   @Override
   public String getLocalName() {
      return streamReader.getLocalName();
   }

   @Override
   public boolean hasName() {
      return streamReader.hasName();
   }

   @Override
   public String getNamespaceURI() {
      return streamReader.getNamespaceURI();
   }

   @Override
   public String getPrefix() {
      return streamReader.getPrefix();
   }

   @Override
   public String getVersion() {
      return streamReader.getVersion();
   }

   @Override
   public boolean isStandalone() {
      return streamReader.isStandalone();
   }

   @Override
   public boolean standaloneSet() {
      return streamReader.standaloneSet();
   }

   @Override
   public String getCharacterEncodingScheme() {
      return streamReader.getCharacterEncodingScheme();
   }

   @Override
   public String getPITarget() {
      return streamReader.getPITarget();
   }

   @Override
   public String getPIData() {
      return streamReader.getPIData();
   }

   @Override
   public String[] getListAttributeValue(int i) {
      return getAttributeValue(i).split("\\s+");
   }

   @Override
   public Schema getSchema() {
      return schema;
   }

   @Override
   public void setSchema(Schema schema) {
      this.schema = schema;
   }

   @Override
   public Properties getProperties() {
      return properties;
   }

   // private members

   private static final class Context {
      int depth = 1;
   }

   private void safeClose() {
      try {
         streamReader.close();
      } catch (Exception e) {
         // ignore
      }
   }

   private static XMLStreamException readPastEnd(final Location location) {
      return new XMLStreamException("Attempt to read past end of element", location);
   }
}
