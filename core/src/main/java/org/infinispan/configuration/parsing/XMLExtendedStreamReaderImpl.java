package org.infinispan.configuration.parsing;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.infinispan.commons.util.Util;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @author Tristan Tarrant
 * @since 6.0
 */
final class XMLExtendedStreamReaderImpl implements XMLExtendedStreamReader {
   private final XMLInputFactory factory;
   final XMLResourceResolver resourceResolver;
   private final NamespaceMappingParser parser;
   private XMLStreamReader streamReader;
   private final Deque<Included> includeStack;
   private final Deque<Context> stack;
   private Schema schema;
   private Properties properties;

   XMLExtendedStreamReaderImpl(XMLInputFactory factory, XMLResourceResolver resourceResolver, final NamespaceMappingParser parser, final XMLStreamReader streamReader, Properties properties) {
      this(factory, resourceResolver, parser, streamReader, properties, new ArrayDeque<>());
      stack.push(new Context());
   }

   XMLExtendedStreamReaderImpl(XMLInputFactory factory, XMLResourceResolver resourceResolver, final NamespaceMappingParser parser, final XMLStreamReader streamReader, Properties properties, Deque<Context> stack) {
      this.factory = factory;
      this.resourceResolver = resourceResolver;
      this.parser = parser;
      this.streamReader = streamReader;
      this.includeStack = new ArrayDeque<>();
      this.properties = properties;
      this.stack = stack;
   }

   @Override
   public void handleAny(final ConfigurationBuilderHolder holder) throws XMLStreamException {
      require(START_ELEMENT, null, null);
      final Deque<Context> stack = this.stack;
      stack.push(new Context());
      try {
         parser.parseElement(this, holder);
      } finally {
         stack.pop();
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
            if ("include".equals(getLocalName())) {
               return next(); // recurse
            }
         } else if (next == START_ELEMENT) {
            context.depth++;
            if ("include".equals(getLocalName())) {
               include();
               return next(); // recurse
            }
         } else if (next == END_DOCUMENT && !includeStack.isEmpty()) {
            closeInclude();
            return next();
         }
         return next;
      } else {
         throw readPastEnd(getLocation());
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
         final int next = nextTag(streamReader);
         if (next == END_ELEMENT) {
            context.depth--;
            if ("include".equals(getLocalName())) {
               return nextTag(); // recurse
            }
         } else if (next == START_ELEMENT) {
            context.depth++;
            if ("include".equals(getLocalName())) {
               include();
               return nextTag(); // recurse
            }
         } else if (next == END_DOCUMENT && !includeStack.isEmpty()) {
            closeInclude();
            return nextTag(); // recurse
         }
         return next;
      } else {
         throw readPastEnd(getLocation());
      }
   }

   private int nextTag(XMLStreamReader reader) throws XMLStreamException {
      if (includeStack.isEmpty()) {
         return streamReader.nextTag();
      } else {
         // Special handling that allows an END_DOCUMENT to be
         int eventType = reader.next();
         while ((eventType == XMLStreamConstants.CHARACTERS && isWhiteSpace()) // skip whitespace
               || (eventType == XMLStreamConstants.CDATA && isWhiteSpace())
               // skip whitespace
               || eventType == XMLStreamConstants.SPACE
               || eventType == XMLStreamConstants.PROCESSING_INSTRUCTION
               || eventType == XMLStreamConstants.COMMENT) {
            eventType = next();
         }
         if (eventType != START_ELEMENT && eventType != END_ELEMENT && eventType != END_DOCUMENT) {
            throw new XMLStreamException("found: " + eventType + ", expected 1, 2 or 8",
                  getLocation());
         }
         return eventType;
      }
   }

   @Override
   public boolean hasNext() throws XMLStreamException {
      if (stack.getFirst().depth > 0) {
         if (streamReader.hasNext()) {
            return true;
         } else if (!includeStack.isEmpty()) {
            closeInclude();
            return hasNext();
         } else {
            return false;
         }
      } else {
         return false;
      }
   }

   @Override
   public void close() throws XMLStreamException {
      while (!includeStack.isEmpty()) {
         closeInclude();
      }
      streamReader.close();
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

   @Override
   public XMLResourceResolver getResourceResolver() {
      return resourceResolver;
   }

   // private members

   private static final class Context {
      int depth = 1;
   }

   private static final class Included {
      InputStream inputStream;
      XMLStreamReader reader;

      Included(InputStream inputStream, XMLStreamReader reader) {
         this.inputStream = inputStream;
         this.reader = reader;
      }
   }

   private void include() throws XMLStreamException {
      String href = getAttributeValue(null, "href");
      try {
         URL url = resourceResolver.resolveResource(href);
         InputStream inputStream = url.openStream();
         XMLStreamReader subReader = factory.createXMLStreamReader(inputStream);
         includeStack.push(new Included(inputStream, streamReader));
         streamReader = new XMLExtendedStreamReaderImpl(factory, new URLXMLResourceResolver(url), parser, subReader, properties, stack);
      } catch (IOException e) {
         throw new XMLStreamException(e);
      }
   }

   private void closeInclude() {
      Included removed = includeStack.pop();
      Util.close(removed.inputStream);
      streamReader = removed.reader;
   }

   private static XMLStreamException readPastEnd(final Location location) {
      return new XMLStreamException("Attempt to read past end of element", location);
   }
}
