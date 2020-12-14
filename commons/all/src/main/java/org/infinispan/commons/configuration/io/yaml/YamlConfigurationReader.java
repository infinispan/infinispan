package org.infinispan.commons.configuration.io.yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.io.AbstractConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationReaderException;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolver;
import org.infinispan.commons.configuration.io.Location;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.configuration.io.PropertyReplacer;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class YamlConfigurationReader extends AbstractConfigurationReader {
   final private Deque<Parsed> state = new ArrayDeque<>();
   final private List<String> attributeNames = new ArrayList<>();
   final private List<String> attributeValues = new ArrayList<>();
   final private List<String> attributeNamespaces = new ArrayList<>();
   final private Map<String, String> namespaces = new HashMap<>();
   private final BufferedReader reader;
   private Parsed next;
   private ElementType type = ElementType.START_DOCUMENT;
   private int line = 0;
   private Node lines;

   public YamlConfigurationReader(Reader reader, ConfigurationResourceResolver resolver, Properties properties, PropertyReplacer replacer, NamingStrategy namingStrategy) {
      super(resolver, properties, replacer, namingStrategy);
      this.reader = reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
      namespaces.put("", ""); // Default namespace
      loadTree();
   }

   private static class Node {
      final Parsed line;
      Deque<Node> children;
      Node parent;

      Node(Parsed line) {
         this.line = line;
      }

      Node addChild(Node child) {
         if (children == null) {
            children = new ArrayDeque<>();
         }
         if (isAttribute(child.line)) {
            children.addFirst(child);
         } else {
            children.addLast(child);
         }
         child.parent = this;
         return child;
      }

      Node addSibling(Node sibling) {
         parent.addChild(sibling);
         return sibling;
      }

      Node next() {
         if (children != null && !children.isEmpty()) {
            return children.removeFirst();
         } else {
            if (parent == null) {
               return null;
            } else {
               return parent.next();
            }
         }
      }

      public String toString() {
         return line.toString();
      }
   }

   private void loadTree() {
      Node current = null;
      do {
         try {
            do {
               next = parseLine(reader.readLine());
            } while (next != null && next.name == null && next.value == null);
         } catch (IOException e) {
            throw new ConfigurationReaderException(e, Location.of(line, 1));
         }
         if (next == null) {
            // EOF
            return;
         }
         if (lines == null) {
            lines = new Node(next);
            current = lines;
         } else {
            if (next.indent == current.line.indent) {
               // Sibling of the current node
               current = current.addSibling(new Node(next));
            } else if (next.indent > current.line.indent) {
               // Child of the current node
               current = current.addChild(new Node(next));
            } else {
               // Find the parent
               while (next.indent <= current.line.indent) {
                  current = current.parent;
               }
               current = current.addChild(new Node(next));
            }
         }
      } while (next != null);
   }

   private static boolean isAttribute(Parsed p) {
      return p.name != null && p.value != null;
   }

   Parsed parseLine(final String s) {
      if (s == null) {
         return null;
      } else {
         Parsed parsed = new Parsed(++line);
         int length = s.length();
         // Trim any space at the end of the line
         while (length > 0 && s.charAt(length - 1) == ' ') length--;
         int state = 0; // 0=INDENT, 1=KEY, 2=COLON, 3=VALUE, 4=TRAILING
         int start = -1;
         for (int i = 0; i < length; i++) {
            int c = s.charAt(i);
            switch (c) {
               case ' ':
                  if (state == 0) {
                     parsed.indent++;
                  } // else ignore: it may be ignorable or part of a key/value
                  break;
               case '\t':
                  if (state == 0) {
                     parsed.indent += 2;
                  } // else ignore: it may be ignorable or part of a key/value
                  break;
               case '#':
                  if (state == 3) {
                     parsed.value = s.substring(start, i - 1).trim();
                     return parsed;
                  } else if (state == 1) {
                     throw new ConfigurationReaderException("Invalid comment", Location.of(line, i));
                  } else {
                     return parsed; // the rest of the  line is a comment
                  }
               case ':':
                  if (i + 1 == length || s.charAt(i + 1) == ' ') {
                     if (state == 1) {
                        parseKey(parsed, s.substring(start, i));
                        state = 2;
                     }
                  }
                  break;
               case '~':
                  if (state == 2 || i + 1 == length) {
                     // the null element
                     parsed.value = null;
                     return parsed;
                  }
               case '\\':
                  if (i + 1 == length) {
                     throw new ConfigurationReaderException("Incomplete escape sequence", Location.of(line, i));
                  } else {
                     i++;
                  }
                  break;
               case '%':
                  if (i == 0) {
                     String[] parts = s.split(" ");
                     if (parts.length == 3 && parts[0].equals("%TAG") && parts[1].startsWith("!") && parts[1].endsWith("!")) {
                        if ("!".equals(parts[1])) {
                           namespaces.put("", parts[2]); // The primary namespace
                        } else {
                           namespaces.put(parts[1].substring(1, parts[1].length() - 1), parts[2]);
                        }
                        return parsed;
                     } else if (parts.length == 2 && parts[0].equals("%YAML")) {
                        return parsed;
                     } else {
                        Log.CONFIG.warn("Unknown directive " + s + " at " + Location.of(line, i));
                     }
                  }
                  break;
               case '-':
                  if (i == 0) {
                     if ("---".equals(s)) {
                        // It's a separator
                        return parsed;
                     }
                  } else if (s.charAt(i + 1) == ' ') {
                     // It's an array delimiter
                     parsed.list = true;
                     i++;
                     if (state == 0) {
                        if (s.charAt(length - 1) != ':') {
                           // It's a bare value
                           state = 2;
                        }
                     }
                  }
                  break;
               case '"':
               case '\'':
                  if (state == 0 || state == 2) {
                     int endQuote = s.indexOf(c, i + 1);
                     if (endQuote < 0) {
                        throw new ConfigurationReaderException("Missing closing quote", Location.of(line, i));
                     }
                     String v = s.substring(i + 1, endQuote).trim();
                     if (state == 0) {
                        parseKey(parsed, v);
                        state = 2;
                     } else {
                        parsed.value = v;
                        state = 4;
                     }
                     i = endQuote + 1; // Skip
                     break;
                  }
                  // Fallthrough
               default:
                  if (state == 0) {
                     state = 1;
                     start = i;
                  } else if (state == 2) {
                     state = 3;
                     start = i;
                  }
            }
         }
         if (state == 1) { // Unterminated key
            throw new ConfigurationReaderException("Incomplete line", Location.of(line, 1));
         }
         if (state == 3) { // we reached the end of the line
            parsed.value = s.substring(start).trim();
         }
         return parsed;
      }
   }

   private void parseKey(Parsed p, String s) {
      int colon = s.lastIndexOf(':');
      p.name = namingStrategy.convert(colon < 0 ? s : s.substring(colon + 1));
      p.nsPrefix = colon < 0 ? "" : s.substring(0, colon);
      if (!p.nsPrefix.isEmpty()) {
         namespaces.putIfAbsent(p.nsPrefix, namingStrategy.convert(p.nsPrefix));
      }
   }

   private void readNext() {
      do {
         if (lines != null) {
            next = lines.line;
            lines = lines.next();
         } else {
            next = null;
         }
      } while (next != null && next.name == null && next.value == null);
   }

   @Override
   public ElementType nextElement() {
      if (next == null) {
         readNext();
      }
      resetAttributes();
      if (next == null) { // If still null, we've reached the end of the document, we need to unwind the stack
         if (state.isEmpty()) {
            type = ElementType.END_DOCUMENT;
         } else {
            if (type == ElementType.END_ELEMENT) {
               state.pop();
            }
            type = state.isEmpty() ? ElementType.END_DOCUMENT : ElementType.END_ELEMENT;
         }
      } else {
         if (!state.isEmpty()) {
            if (next.indent < state.peek().indent) {
               if (type == ElementType.END_ELEMENT) {
                  state.pop();
               }
               type = ElementType.END_ELEMENT;
               return type;
            } else if (next.indent == state.peek().indent) {
               if (type != ElementType.END_ELEMENT) {
                  // Emit the end element for the current item
                  type = ElementType.END_ELEMENT;
                  return type;
               } else {
                  state.pop();
               }
            } else if (next.list && state.peek().list) {
               if (type != ElementType.END_ELEMENT) {
                  // Emit the end element for the current item
                  type = ElementType.END_ELEMENT;
                  return type;
               } else {
                  // Next element in the list
                  state.peek().value = next.value;
                  readNext();
                  type = ElementType.START_ELEMENT;
                  return type;
               }
            }
         }
         state.push(next);
         int currentIndent = next.indent;
         readNext();
         if (next != null && next.name == null && next.list) {
            // Value list
            Parsed current = state.peek();
            current.list = true;
            current.value = next.value;
            readNext();
         } else {
            // Read the attributes: they are indented relative to the element and have a value
            while (next != null && next.indent > currentIndent && next.name != null && next.value != null) {
               this.attributeNames.add(next.name);
               this.attributeNamespaces.add(next.nsPrefix);
               this.attributeValues.add(replaceProperties(next.value));
               readNext();
            }
         }
         type = ElementType.START_ELEMENT;
      }
      return type;
   }

   private void resetAttributes() {
      this.attributeNames.clear();
      this.attributeValues.clear();
      this.attributeNamespaces.clear();
   }

   @Override
   public Location getLocation() {
      return Location.of(line, 1);
   }

   @Override
   public String getAttributeName(int index, NamingStrategy strategy) {
      return strategy.convert(attributeNames.get(index));
   }

   @Override
   public String getAttributeNamespace(int index) {
      return namespaces.get(attributeNamespaces.get(index));
   }

   @Override
   public String getAttributeValue(String name) {
      for (int i = 0; i < attributeNames.size(); i++) {
         if (name.equals(attributeNames.get(i))) {
            return attributeValues.get(i);
         }
      }
      return null;
   }

   @Override
   public String getAttributeValue(int index) {
      return attributeValues.get(index);
   }

   @Override
   public String getElementText() {
      return replaceProperties(state.peek().value);
   }

   @Override
   public String getLocalName(NamingStrategy strategy) {
      return strategy.convert(state.peek().name);
   }

   @Override
   public String getNamespace() {
      return namespaces.get(state.peek().nsPrefix);
   }

   @Override
   public boolean hasNext() {
      if (!state.isEmpty()) {
         return true;
      }
      if (next == null) {
         readNext();
      }
      return next != null;
   }

   @Override
   public int getAttributeCount() {
      return attributeNames.size();
   }

   @Override
   public void require(ElementType type, String namespace, String name) {
      if (type != this.type
            || (namespace != null && !namespace.equals(getNamespace()))
            || (name != null && !name.equals(getLocalName()))) {
         throw new ConfigurationReaderException("Expected event " + type
               + (name != null ? " with name '" + name + "'" : "")
               + (namespace != null && name != null ? " and" : "")
               + (namespace != null ? " with namespace '" + namespace + "'" : "")
               + " but got"
               + (type != this.type ? " " + this.type : "")
               + (name != null && getLocalName() != null && !name.equals(getLocalName())
               ? " name '" + getLocalName() + "'" : "")
               + (namespace != null && name != null
               && getLocalName() != null && !name.equals(getLocalName())
               && getNamespace() != null && !namespace.equals(getNamespace())
               ? " and" : "")
               + (namespace != null && getNamespace() != null && !namespace.equals(getNamespace())
               ? " namespace '" + getNamespace() + "'" : ""), Location.of(line, 1));
      }
   }

   @Override
   public void close() throws Exception {
      Util.close(reader);
   }

   public static class Parsed {
      final int line;
      int indent;
      boolean list;
      String name;
      String nsPrefix;
      String value;

      public Parsed(int line) {
         this.line = line;
      }

      @Override
      public String toString() {
         return "Parsed{" +
               "line=" + line +
               ", indent=" + indent +
               ", list=" + list +
               ", name='" + name + '\'' +
               ", nsPrefix='" + nsPrefix + '\'' +
               ", value='" + value + '\'' +
               '}';
      }
   }
}
