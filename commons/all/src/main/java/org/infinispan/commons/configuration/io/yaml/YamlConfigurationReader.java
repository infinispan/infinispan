package org.infinispan.commons.configuration.io.yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.SimpleImmutableEntry;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class YamlConfigurationReader extends AbstractConfigurationReader {
   private static final int INDENT = 2;
   private final Deque<Parsed> state = new ArrayDeque<>();
   private final List<String> attributeNames = new ArrayList<>();
   private final List<String> attributeValues = new ArrayList<>();
   private final List<String> attributeNamespaces = new ArrayList<>();
   private final Map<String, String> namespaces = new HashMap<>();
   private final BufferedReader reader;
   private Parsed next;
   private ElementType type = ElementType.START_DOCUMENT;
   private int row = 0;
   private int column = 0;
   private Node lines;

   public YamlConfigurationReader(Reader reader, ConfigurationResourceResolver resolver, Properties properties, PropertyReplacer replacer, NamingStrategy namingStrategy) {
      super(resolver, properties, replacer, namingStrategy);
      this.reader = reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
      namespaces.put("", ""); // Default namespace
      loadTree();
      if (Log.CONFIG.isTraceEnabled()) {
         StringWriter sw = new StringWriter();
         PrintWriter pw = new PrintWriter(sw);
         printTree(pw, lines);
         Log.CONFIG.trace(sw);
      }
   }

   private void printTree(PrintWriter pw, Node node) {
      if (node != null) {

         for (int i = 0; i < node.parsed.indent; i++) {
            pw.print(' ');
         }
         pw.println(node.parsed);
         if (node.children != null) {
            for (Node c : node.children) {
               printTree(pw, c);
            }
         }
      }
   }

   private static class Node {
      final Parsed parsed;
      ArrayList<Node> children;
      Node parent;

      Node(Parsed parsed) {
         this.parsed = parsed;
      }

      Node addChild(Node child) {
         if (children == null) {
            children = new ArrayList<>();
         }
         if (isAttribute(child.parsed)) {
            // Add before the first non attribute
            for (int i = 0; i < children.size(); i++) {
               if (!isAttribute(children.get(i).parsed)) {
                  children.add(i, child);
                  child.parent = this;
                  return child;
               }
            }
         }
         children.add(child);
         child.parent = this;
         return child;
      }

      Node addSibling(Node sibling) {
         parent.addChild(sibling);
         return sibling;
      }

      Node next() {
         if (hasChildren()) {
            return children.remove(0);
         } else {
            if (parent == null) {
               return null;
            } else {
               return parent.next();
            }
         }
      }

      public String toString() {
         return parsed.toString();
      }

      boolean hasChildren() {
         return children != null && !children.isEmpty();
      }
   }

   private void loadTree() {
      Parsed parsed;
      Node current = null;
      do {
         try {
            do {
               parsed = parseLine(reader.readLine());
            } while (parsed != null && parsed.name == null && parsed.value == null && !parsed.list);
         } catch (IOException e) {
            throw new ConfigurationReaderException(e, new Location(getName(), row, column));
         }
         if (parsed == null) {
            // EOF
            return;
         }
         if (lines == null) {
            if (parsed.name == null) {
               throw new ConfigurationReaderException("Incomplete line", new Location(getName(), row, column));
            }
            lines = new Node(parsed);
            current = lines;
         } else {
            if (parsed.list) {
               if (parsed.name == null) {
                  // It's an array
                  if (current.parsed.indent == parsed.indent) {
                     current = current.addSibling(new Node(parsed));
                  } else {
                     current = current.addChild(new Node(parsed));
                  }
               } else {
                  // Find the parent of the previous element
                  current = findParent(current, parsed);
                  if (current.parsed.list) {
                     current = current.parent;
                  }
                  // Clone the parent
                  current = addListItem(parsed, current);
                  current = current.addChild(new Node(parsed));
               }
            } else if (parsed.indent == current.parsed.indent) {
               // Sibling of the current node
               current = current.addSibling(new Node(parsed));
            } else if (parsed.indent > current.parsed.indent) {
               // Child of the current node
               if (parsed.name == null) {
                  // It's a value continuation, append it to the current
                  current.parsed.value = current.parsed.value + " " + parsed.value;
               } else {
                  current = current.addChild(new Node(parsed));
               }
            } else {
               // Lower indent than, the current owner, climb up the tree to find the parent
               current = findParent(current, parsed);
               current = current.addChild(new Node(parsed));
            }
         }
      } while (true);
   }

   private Node addListItem(Parsed parsed, Node current) {
      // It's a list item, we create a synthetic node with the same name as the parent
      Parsed holder = new Parsed(parsed.row);
      holder.nsPrefix = current.parsed.nsPrefix;
      holder.name = current.parsed.name;
      holder.indent = current.parsed.indent + 1;//parsed.indent;
      holder.list = true;
      current = current.addChild(new Node(holder));
      // And the parsed line is added as a child of the holder
      parsed.list = false;
      return current;
   }

   private Node findParent(Node current, Parsed line) {
      while (line.indent <= current.parsed.indent) {
         current = current.parent;
      }
      return current;
   }

   private static boolean isAttribute(Parsed p) {
      return p.name != null && p.value != null;
   }

   Parsed parseLine(final String s) {
      if (s == null) {
         return null;
      } else {
         Parsed parsed = new Parsed(++row);
         int length = s.length();
         // Trim any space at the end of the line
         while (length > 0 && s.charAt(length - 1) == ' ') length--;
         int state = 0; // 0=INDENT, 1=KEY, 2=COLON, 3=VALUE, 4=TRAILING
         int start = -1;
         for (int i = 0; i < length; i++) {
            column = i + 1;
            int c = s.charAt(i);
            switch (c) {
               case ' ':
                  if (state == 0) {
                     parsed.indent++;
                  } // else ignore: it may be ignorable or part of a key/value
                  break;
               case '\t':
                  if (state == 0) {
                     parsed.indent += INDENT;
                  } // else ignore: it may be ignorable or part of a key/value
                  break;
               case '#':
                  if (state == 3) {
                     parsed.value = s.substring(start, i - 1).trim();
                     return parsed;
                  } else if (state == 1) {
                     throw new ConfigurationReaderException("Invalid comment", new Location(getName(), row, i));
                  } else {
                     return parsed; // the rest of the  line is a comment
                  }
               case ':':
                  if (i + 1 == length || s.charAt(i + 1) == ' ') {
                     if (state == 1) {
                        if (start >= 0) {
                           parseKey(parsed, s.substring(start, i));
                        }
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
                     throw new ConfigurationReaderException("Incomplete escape sequence", new Location(getName(), row, i));
                  } else {
                     if (state == 2) {
                        state = 3;
                        start = i;
                     }
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
                        Log.CONFIG.warn("Unknown directive " + s + " at " + new Location(getName(), row, i));
                     }
                  }
                  break;
               case '-':
                  if (i == 0 && "---".equals(s)) {
                     // It's a separator
                     return parsed;
                  } else if (state == 0) {
                     // It's a list delimiter
                     parsed.list = true;
                     parsed.indent++;
                  }
                  break;
               case '"':
               case '\'':
                  if (state == 0 || state == 2) {
                     int endQuote = s.indexOf(c, i + 1);
                     if (endQuote < 0) {
                        throw new ConfigurationReaderException("Missing closing quote", new Location(getName(), row, i));
                     }
                     String v = s.substring(i + 1, endQuote).trim();
                     if (state == 0) {
                        parseKey(parsed, v);
                        state = 1;
                     } else {
                        parsed.value = v;
                        state = 4;
                     }
                     i = endQuote; // Skip
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
         if (state == 1) { // Bare string
            if (parsed.list) {
               if (start > 0) {
                  parsed.value = s.substring(start).trim();
               } else {
                  // The value was stored in the name, swap them
                  parsed.value = parsed.name;
                  parsed.name = null;
               }
            } else {
               // It's probably a continuation of the previous line
               parsed.value = unescape(s.substring(start).trim());
            }
         } else if (state == 3) { // we reached the end of the line
            String val = s.substring(start).trim();
            switch (val) {
               // Handle various null values: null | Null | NULL | {}
               case "{}":
               case "null":
               case "Null":
               case "NULL":
                  parsed.value = null;
                  return parsed;
               default:
                  parsed.value = unescape(val);
            }
         }
         return parsed;
      }
   }

   private static String unescape(String s) {
      StringBuilder sb = null; // avoid allocating if there are no escapes
      for (int i = 0; i < s.length(); i++) {
         char ch = s.charAt(i);
         if (ch == '\\') {
            if (sb == null) {
               sb = new StringBuilder(s.substring(0, i));
            }
            i++;
            ch = s.charAt(i);
            ch = switch (ch) {
               case 'b' -> '\b';
               case 'f' -> '\f';
               case 'n' -> '\n';
               case 'r' -> '\r';
               case 't' -> '\t';
               default -> ch;
            };
         }
         if (sb != null) {
            sb.append(ch);
         }
      }
      return sb == null ? s : sb.toString();
   }

   private void parseKey(Parsed p, String s) {
      int colon = s.lastIndexOf(':');
      p.name = colon < 0 ? s : s.substring(colon + 1);
      p.nsPrefix = colon < 0 ? "" : s.substring(0, colon);
      if (!p.nsPrefix.isEmpty()) {
         namespaces.putIfAbsent(p.nsPrefix, namingStrategy.convert(p.nsPrefix));
      }
   }

   private void readNext() {
      do {
         if (lines != null) {
            next = lines.parsed;
            lines = lines.next();
         } else {
            next = null;
         }
      } while (next != null && next.name == null && next.value == null && !next.list);
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
               } else {
                  // Next element in the list
                  state.peek().value = next.value;
                  readNext();
                  type = ElementType.START_ELEMENT;
               }
               return type;
            }
         }
         state.push(next);
         int currentIndent = next.indent;
         readNext();
         if (next != null && next.list) {
            if (next.name == null) {
               if (next.value != null) {
                  // Bare value list
                  Parsed current = state.peek();
                  current.list = true;
                  current.value = next.value;
                  readNext();
               } else {
                  // Nested array
                  Parsed current = state.peek();
                  next.name = current.name;
               }
            }
         } else {
            // Read the attributes: they are indented relative to the element and have a value
            while (next != null && next.indent > currentIndent && next.name != null) {
               if (next.value != null) {
                  setAttributeValue(next.nsPrefix, next.name, next.value);
                  readNext();
               } else {
                  if (lines != null && lines.parsed.list && lines.parsed.name == null && lines.parsed.value != null) {
                     String name = next.name;
                     String namespace = next.nsPrefix;
                     StringBuilder sb = new StringBuilder();
                     readNext();
                     while (next != null && next.list) {
                        sb.append(replaceProperties(next.value)).append(' ');
                        readNext();
                     }
                     this.setAttributeValue(namespace, name, sb.toString());
                  } else {
                     break;
                  }
               }
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
      return new Location(getName(), row, column);
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
   public String getAttributeValue(String localName, NamingStrategy strategy) {
      for (int i = 0; i < attributeNames.size(); i++) {
         if (localName.equals(strategy.convert(attributeNames.get(i)))) {
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
   public Map.Entry<String, String> getMapItem(String nameAttribute) {
      String name = getLocalName(NamingStrategy.IDENTITY);
      nextElement();
      String type = getLocalName();
      return new SimpleImmutableEntry<>(name, type);
   }

   @Override
   public void endMapItem() {
      nextElement();
   }

   @Override
   public String[] readArray(String outer, String inner) {
      require(ElementType.START_ELEMENT, null, outer);
      List<String> elements = new ArrayList<>();
      boolean loop;
      do {
         elements.add(getElementText());
         nextElement();
         require(ElementType.END_ELEMENT, null, outer);
         loop = next.list;
         if (loop) {
            nextElement();
         }
      } while (loop);
      return elements.toArray(new String[0]);
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
               ? " namespace '" + getNamespace() + "'" : ""), new Location(getName(), row, 1));
      }
   }

   @Override
   public boolean hasFeature(ConfigurationFormatFeature feature) {
      return false;
   }

   @Override
   public void close() {
      Util.close(reader);
   }

   @Override
   public void setAttributeValue(String namespace, String name, String value) {
      this.attributeNames.add(name);
      this.attributeNamespaces.add(namespace);
      this.attributeValues.add(replaceProperties(value));
   }

   public Map<String, Object> asMap() {
      return Collections.singletonMap(lines.parsed.name, asMap(lines));
   }

   private Object asMap(Node node) {
      if (node.hasChildren()) {
         if (node.children.get(0).parsed.list) {
            List<Object> children = new ArrayList<>(node.children.size());
            for (Node child : node.children) {
               children.add(asMap(child));
            }
            return children;
         } else {
            Map<String, Object> children = new LinkedHashMap<>(node.children.size());
            for (Node child : node.children) {
               children.put(child.parsed.name, asMap(child));
            }
            return children;
         }
      } else {
         return node.parsed.value;
      }
   }

   public static class Parsed {
      final int row;
      int indent;
      boolean list;
      String name;
      String nsPrefix;
      String value;

      public Parsed(int row) {
         this.row = row;
      }

      @Override
      public String toString() {
         return "{[" + row + "," + indent + "] " + nsPrefix + ":" + name + (list ? "[]" : "") + (value == null ? ":" : (": " + value)) + "}";
      }
   }
}
