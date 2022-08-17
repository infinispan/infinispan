package org.infinispan.commons.configuration.io.xml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.io.AbstractConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
import org.infinispan.commons.configuration.io.ConfigurationReaderException;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolver;
import org.infinispan.commons.configuration.io.Location;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.configuration.io.PropertyReplacer;
import org.infinispan.commons.configuration.io.URLConfigurationResourceResolver;
import org.infinispan.commons.util.SimpleImmutableEntry;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class XmlConfigurationReader extends AbstractConfigurationReader {
   public static final String XINCLUDE = "include";
   public static final String XINCLUDE_NS = "http://www.w3.org/2001/XInclude";
   private final Deque<State> stack;
   private State state;
   int token;

   public XmlConfigurationReader(Reader reader, ConfigurationResourceResolver resolver, Properties properties, PropertyReplacer replacer, NamingStrategy namingStrategy) {
      this(reader, resolver, properties, replacer, namingStrategy, new ArrayDeque<>());
   }

   private XmlConfigurationReader(Reader reader, ConfigurationResourceResolver resolver, Properties properties, PropertyReplacer replacer, NamingStrategy namingStrategy, Deque<State> stack) {
      super(resolver, properties, replacer, namingStrategy);
      this.state = new State(reader, getParser(reader), resolver);
      this.stack = stack;
      this.stack.push(state);
      token = -1;
   }

   private MXParser getParser(Reader reader) {
      MXParser parser = new MXParser(reader);
      parser.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);
      return parser;
   }

   @Override
   public ConfigurationResourceResolver getResourceResolver() {
      return state.resolver;
   }

   @Override
   public void require(ElementType elementType, String namespace, String name) {
      int type;
      switch (elementType) {
         case START_DOCUMENT:
            type = XmlPullParser.START_DOCUMENT;
            break;
         case END_DOCUMENT:
            type = XmlPullParser.END_DOCUMENT;
            break;
         case START_ELEMENT:
            type = XmlPullParser.START_TAG;
            break;
         case END_ELEMENT:
            type = XmlPullParser.END_TAG;
            break;
         default:
            throw new IllegalArgumentException(elementType.name());
      }
      try {
         state.parser.require(type, namespace, name);
      } catch (IOException e) {
         throw new ConfigurationReaderException(e, getLocation());
      }
   }

   @Override
   public boolean hasNext() {
      if (token < 0) {
         try {
            token = state.parser.next();
            if (token == XmlPullParser.END_DOCUMENT) {
               token = closeInclude();
            }
         } catch (IOException e) {
            throw new ConfigurationReaderException(e, getLocation());
         }
      }
      return token != XmlPullParser.END_DOCUMENT;
   }

   private int nextEvent() {
      try {
         int event = token < 0 ? state.parser.next() : token;
         token = -1;
         for (; ; ) {
            if (event == XmlPullParser.START_TAG && XINCLUDE.equals(getLocalName()) && XINCLUDE_NS.equals(getNamespace())) {
               event = include();
            } else if (event == XmlPullParser.END_TAG && XINCLUDE.equals(getLocalName()) && XINCLUDE_NS.equals(getNamespace())) {
               event = closeInclude();
            } else if (event == XmlPullParser.END_DOCUMENT) {
               event = closeInclude();
               if (event == XmlPullParser.END_DOCUMENT) {
                  return event;
               }
            } else {
               return event;
            }
         }
      } catch (XmlPullParserException | IOException e) {
         throw new ConfigurationReaderException(e, getLocation());
      }
   }

   @Override
   public ElementType nextElement() {
      int event = nextEvent();
      while (event == XmlPullParser.TEXT
            || event == XmlPullParser.IGNORABLE_WHITESPACE
            || event == XmlPullParser.PROCESSING_INSTRUCTION
            || event == XmlPullParser.COMMENT) {
         event = nextEvent();
      }

      switch (event) {
         case XmlPullParser.START_DOCUMENT:
            return ElementType.START_DOCUMENT;
         case XmlPullParser.END_DOCUMENT:
            return ElementType.END_DOCUMENT;
         case XmlPullParser.START_TAG:
            return ElementType.START_ELEMENT;
         case XmlPullParser.END_TAG:
            return ElementType.END_ELEMENT;
         default:
            throw new ConfigurationReaderException("Expecting event type >=1 <=4, got " + event, getLocation());
      }
   }

   @Override
   public String getLocalName(NamingStrategy strategy) {
      return strategy.convert(state.parser.getName());
   }

   @Override
   public String getNamespace() {
      return state.parser.getNamespace();
   }

   @Override
   public int getAttributeCount() {
      return state.parser.getAttributeCount();
   }

   @Override
   public String getAttributeName(int index, NamingStrategy strategy) {
      return strategy.convert(state.parser.getAttributeName(index));
   }

   @Override
   public String getAttributeValue(int index) {
      String value = state.parser.getAttributeValue(index);
      return replaceProperties(value);
   }

   @Override
   public String getAttributeValue(String localName, NamingStrategy strategy) {
      String value = state.parser.getAttributeValue(null, strategy.convert(localName));
      return replaceProperties(value);
   }

   @Override
   public String getElementText() {
      try {
         return replaceProperties(state.parser.nextText().trim());
      } catch (IOException e) {
         throw new ConfigurationReaderException("Expected text", getLocation());
      }
   }

   @Override
   public Location getLocation() {
      return Location.of(state.parser.getLineNumber(), state.parser.getColumnNumber());
   }

   @Override
   public String getAttributeNamespace(int index) {
      return state.parser.getAttributeNamespace(index);
   }

   @Override
   public Map.Entry<String, String> getMapItem(String nameAttribute) {
      String type = getLocalName();
      String name = getAttributeValue(nameAttribute);
      return new SimpleImmutableEntry<>(name, type);
   }

   @Override
   public void endMapItem() {
      // Do nothing
   }

   @Override
   public String[] readArray(String outer, String inner) {
      List<String> list = new ArrayList<>();
      while (inTag(outer)) {
         if (inner.equals(getLocalName())) {
            list.add(getElementText());
         } else {
            throw new ConfigurationReaderException(getLocalName(), getLocation());
         }
      }
      return list.toArray(new String[0]);
   }

   private int include() {
      try {
         String href = getAttributeValue("href");
         URL url = state.resolver.resolveResource(href);
         BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
         state = new State(reader, getParser(reader), new URLConfigurationResourceResolver(url));
         stack.push(state);
         require(ElementType.START_DOCUMENT);
         int tag = state.parser.nextTag();
         require(ElementType.START_ELEMENT);
         return tag;
      } catch (IOException e) {
         throw new ConfigurationReaderException(e, getLocation());
      }
   }


   private int closeInclude() {
      try {
         if (stack.size() > 1) {
            State removed = stack.pop();
            Util.close(removed.reader);
            state = stack.peek();
            state.parser.nextTag();
            require(ElementType.END_ELEMENT);
            return state.parser.nextTag();
         } else {
            return XmlPullParser.END_DOCUMENT;
         }
      } catch (IOException e) {
         throw new ConfigurationReaderException(e, getLocation());
      }
   }

   @Override
   public boolean hasFeature(ConfigurationFormatFeature feature) {
      switch (feature) {
         case MIXED_ELEMENTS:
         case BARE_COLLECTIONS:
            return true;
         default:
            return false;
      }
   }

   @Override
   public void close() {
      Util.close(state.reader);
   }

   @Override
   public void setAttributeValue(String namespace, String name, String value) {}

   // private members
   private static final class State {
      Reader reader;
      XmlPullParser parser;
      ConfigurationResourceResolver resolver;

      State(Reader reader, MXParser parser, ConfigurationResourceResolver resolver) {
         this.reader = reader;
         this.parser = parser;
         this.resolver = resolver;
      }
   }
}
