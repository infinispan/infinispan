package org.infinispan.configuration.parsing;

import org.infinispan.commons.util.BeanUtils;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;

/**
 * A simple XML utility class for reading configuration elements
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class XmlConfigHelper {
   private static final Log log = LogFactory.getLog(XmlConfigHelper.class);

   /**
    * Returns the contents of a specific node of given element name, provided a certain attribute exists and is set to
    * value. E.g., if you have a {@link Element} which represents the following XML snippet:
    * <pre>
    *   &lt;ItemQuantity Colour="Red"&gt;100&lt;/ItemQuantity&gt;
    *   &lt;ItemQuantity Colour="Blue"&gt;30&lt;/ItemQuantity&gt;
    *   &lt;ItemQuantity Colour="Black"&gt;10&lt;/ItemQuantity&gt;
    * <pre>
    * <p/>
    * The following results could be expected:
    * </p>
    * <pre>
    *    getTagContents(element, "Red", "ItemQuantity", "Colour"); // 100
    *    getTagContents(element, "Black", "ItemQuantity", "Colour"); // 10
    *    getTagContents(element, "Blah", "ItemQuantity", "Colour"); // null
    *    getTagContents(element, "Red", "Blah", "Colour"); // null
    *    getTagContents(element, "Black", "ItemQuantity", "Blah"); // null
    * </pre>
    * <p/>
    * None of the parameters should be null - otherwise the method may throw a NullPointerException.
    * </p>
    *
    * @param elem          - element to search through.
    * @param value         - expected value to match against
    * @param elementName   - element name
    * @param attributeName - attribute name of the element that would contain the expected value.
    * @return the contents of the matched element, or null if not found/matched
    */
   public static String getTagContents(Element elem, String value, String elementName, String attributeName) {
      NodeList list = elem.getElementsByTagName(elementName);

      for (int s = 0; s < list.getLength(); s++) {
         org.w3c.dom.Node node = list.item(s);
         if (node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE)
            continue;

         Element element = (Element) node;
         String name = element.getAttribute(attributeName);
         if (name.equals(value)) {
            return getElementContent(element, true);
         }
      }
      return null;
   }

   /**
    * Retrieves the value of a given attribute for the first encountered instance of a tag in an element. <p/> E.g., if
    * you have a {@link Element} which represents the following XML snippet: </p>
    * <pre>
    *   &lt;ItemQuantity Colour="Red"&gt;100&lt;/ItemQuantity&gt;
    *   &lt;ItemQuantity Colour="Blue"&gt;30&lt;/ItemQuantity&gt;
    *   &lt;ItemQuantity Colour="Black"&gt;10&lt;/ItemQuantity&gt;
    * <pre>
    * <p/>
    * The following results could be expected:
    * </p>
    * <pre>
    *    getAttributeValue(element, "ItemQuantity", "Colour"); // "Red"
    *    getTagContents(element, "Blah", "Colour"); // null
    *    getTagContents(element, "ItemQuantity", "Blah"); // null
    * </pre>
    * None of the parameters should be null - otherwise the method may throw a NullPointerException.
    *
    * @param elem          - element to search through.
    * @param elementName   - element name
    * @param attributeName - attribute name of the element that would contain the expected value.
    * @return the contents of the matched attribute, or null if not found/matched
    */
   public static String getAttributeValue(Element elem, String elementName, String attributeName) {
      NodeList list = elem.getElementsByTagName(elementName);

      for (int s = 0; s < list.getLength(); s++) {
         org.w3c.dom.Node node = list.item(s);
         if (node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE)
            continue;

         Element element = (Element) node;
         String value = element.getAttribute(attributeName);
         return value == null ? null : StringPropertyReplacer.replaceProperties(value);

      }
      return null;
   }

   /**
    * Returns a named sub-element of the current element passed in.
    * <p/>
    * None of the parameters should be null - otherwise the method may throw a NullPointerException.
    *
    * @param element        - element to search through.
    * @param subElementName - the name of a sub element to look for
    * @return the first matching sub element, if found, or null otherwise.
    */
   public static Element getSubElement(Element element, String subElementName) {
      NodeList nl = element.getChildNodes();
      for (int i = 0; i < nl.getLength(); i++) {
         Node node = nl.item(i);
         if (node.getNodeType() == Node.ELEMENT_NODE && subElementName.equals(((Element) node).getTagName())) {
            return (Element) node;
         }
      }

      if (log.isDebugEnabled()) log.debugf("getSubElement(): Does not exist for %s", subElementName);
      return null;
   }

   /**
    * Reads the contents of the element passed in.
    * <p/>
    * None of the parameters should be null - otherwise the method may throw a NullPointerException.
    *
    * @param element - element to search through.
    * @param trim    - if true, whitespace is trimmed before returning
    * @return the contents of the element passed in.  Will return an empty String if the element is empty.
    */
   public static String getElementContent(Element element, boolean trim) {
      NodeList nl = element.getChildNodes();
      StringBuilder attributeText = new StringBuilder();
      for (int i = 0; i < nl.getLength(); i++) {
         Node n = nl.item(i);
         if (n instanceof Text) {
            attributeText.append(StringPropertyReplacer.replaceProperties(((Text) n).getData()));
         }
      } // end of for ()
      if (trim)
         return attributeText.toString().trim();
      return attributeText.toString();
   }

   /**
    * Reads the contents of the first occurrence of elementName under the given element, trimming results of whitespace.
    * <p/>
    * None of the parameters should be null - otherwise the method may throw a NullPointerException.
    *
    * @param element     - element to search through.
    * @param elementName - name of the element to find within the element passed in
    * @return may return an empty String of not found.
    */
   public static String readStringContents(Element element, String elementName) {
      NodeList nodes = element.getElementsByTagName(elementName);
      if (nodes.getLength() > 0) {
         Node node = nodes.item(0);
         Element ne = (Element) node;
         NodeList nl2 = ne.getChildNodes();
         Node node2 = nl2.item(0);
         if (node2 != null) {
            String value = node2.getNodeValue();
            if (value == null)
               return "";
            return StringPropertyReplacer.replaceProperties(value.trim());
         } else {
            return "";
         }
      } else {
         return "";
      }
   }

   /**
    * Escapes backslashes ('\') with additional backslashes in a given String, returning a new, escaped String.
    *
    * @param value String to escape.   Cannot be null.
    * @return escaped String.  Never is null.
    */
   public static String escapeBackslashes(String value) {
      StringBuilder buf = new StringBuilder(value);
      for (int looper = 0; looper < buf.length(); looper++) {
         char curr = buf.charAt(looper);
         char next = 0;
         if (looper + 1 < buf.length())
            next = buf.charAt(looper + 1);

         if (curr == '\\') {
            if (next != '\\') {           // only if not already escaped
               buf.insert(looper, '\\');  // escape backslash
            }
            looper++;                    // skip past extra backslash (either the one we added or existing)
         }
      }
      return buf.toString();
   }

   /**
    * Reads the contents of a named sub element within a given element, and attempts to parse the contents as a Java
    * properties file.
    * <p/>
    * E.g., if you have a {@link Element} which represents the following XML snippet:
    * <p/>
    * <pre>
    *   &lt;props&gt;
    *       my.attrib.1 = blah
    *       my.attrib.2 = blahblah
    *   &lt;/props&gt;
    * <pre>
    * <p/>
    * The following results could be expected:
    * <p/>
    * <pre>
    *    Properties p = readPropertiesContents(element, "props");
    *    p.getProperty("my.attrib.1"); // blah
    *    p.getProperty("my.attrib.2"); // blahblah
    * </pre>
    * None of the parameters should be null - otherwise the method may throw a NullPointerException.
    *
    * @param element     - element to search through.
    * @param elementName - name of the element to find within the element passed in
    * @return a {@link Properties} object, never null.
    * @throws IOException if unable to parse the contents of the element
    */
   public static Properties readPropertiesContents(Element element, String elementName) {
      String stringContents = readStringContents(element, elementName);
      if (stringContents == null) return new Properties();
      stringContents = escapeBackslashes(stringContents);
      ByteArrayInputStream is;
      Properties properties;
      try {
         is = new ByteArrayInputStream(stringContents.trim().getBytes("ISO8859_1"));
         properties = new Properties();
         properties.load(is);
         is.close();
      }
      catch (IOException e) {
         log.errorReadingProperties(e);
         throw new CacheConfigurationException("Exception occured while reading properties from XML document", e);
      }
      return properties;
   }

   public static Properties readPropertiesContents(Element element) {
      return readPropertiesContents(element, "properties");
   }

   /**
    * Similar to {@link #readStringContents(org.w3c.dom.Element,String)} except that it returns a boolean.
    *
    * @param element     - element to search through.
    * @param elementName - name of the element to find within the element passed in
    * @return the contents of the element as a boolean, or false if not found.
    */
   public static boolean readBooleanContents(Element element, String elementName) {
      return readBooleanContents(element, elementName, false);
   }

   /**
    * Similar to {@link #readStringContents(org.w3c.dom.Element,String)} except that it returns a boolean.
    *
    * @param element      - element to search through.
    * @param elementName  - name of the element to find within the element passed in
    * @param defaultValue - value to return if the element is not found or cannot be parsed.
    * @return the contents of the element as a boolean
    */
   public static boolean readBooleanContents(Element element, String elementName, boolean defaultValue) {
      String val = readStringContents(element, elementName);
      if (val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false")) {
         // needs to be done this way because of JBBUILD-351
         return Boolean.valueOf(val);
         //return Boolean.parseBoolean(val);
      }
      return defaultValue;
   }

   /**
    * Converts a String representing an XML snippet into an {@link org.w3c.dom.Element}.
    *
    * @param xml snippet as a string
    * @return a DOM Element
    * @throws Exception if unable to parse the String or if it doesn't contain valid XML.
    */
   public static Element stringToElement(String xml) throws Exception {
      ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes("utf8"));
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document d = builder.parse(bais);
      bais.close();
      return d.getDocumentElement();
   }

   /**
    * Gets the first child element of an element
    *
    * @param element the parent
    * @return the first child element or null if there isn't one
    */
   public static Element getFirstChildElement(Element element) {
      Node child = element.getFirstChild();
      while (child != null && child.getNodeType() != Node.ELEMENT_NODE)
         child = child.getNextSibling();

      return (Element) child;
   }

   /**
    * Returns the root element of a given input stream
    *
    * @param is stream to parse
    * @return XML DOM element, or null if unable to parse stream
    */
   public static Element getDocumentRoot(InputStream is) {
      Document doc;
      try {
         InputSource xmlInp = new InputSource(is);

         DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
         docBuilderFactory.setNamespaceAware(true);
         DocumentBuilder parser = docBuilderFactory.newDocumentBuilder();
         doc = parser.parse(xmlInp);
         Element root = doc.getDocumentElement();
         root.normalize();
         return root;
      }
      catch (SAXParseException err) {
         log.configuratorSAXParseError(err);
      }
      catch (SAXException e) {
         log.configuratorSAXError(e);
      }
      catch (Exception pce) {
         log.configuratorError(pce);
      }
      return null;
   }

   /**
    * Retrieves the boolean value of a given attribute for the first encountered instance of elementName
    *
    * @param elem          - element to search
    * @param elementName   - name of element to find
    * @param attributeName - name of attribute to retrieve the value of
    * @param defaultValue  - default value to return if not found
    */
   public static boolean readBooleanAttribute(Element elem, String elementName, String attributeName, boolean defaultValue) {
      String val = getAttributeValue(elem, elementName, attributeName);
      if (val != null) {
         if (val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false")) {
            //return Boolean.parseBoolean(val);
            // needs to be done this way because of JBBUILD-351
            return Boolean.valueOf(val);
         }
      }

      return defaultValue;
   }

   public static void setValues(Object target, Map<?, ?> attribs, boolean isXmlAttribs, boolean failOnMissingSetter) {
      Class<?> objectClass = target.getClass();

      // go thru simple string setters first.
      for (Map.Entry<?, ?> entry : attribs.entrySet()) {
         String propName = (String) entry.getKey();
         String setter = BeanUtils.setterName(propName);
         String fluentSetter = BeanUtils.fluentSetterName(propName);

         try {
            Method method;
            if (isXmlAttribs) {
               method = objectClass.getMethod(setter, Element.class);
               method.invoke(target, entry.getValue());
            } else {
               method = objectClass.getMethod(setter, String.class);
               method.invoke(target, entry.getValue());
            }

            continue;
         }
         catch (NoSuchMethodException me) {
            // this is ok, but certainly log this as a warning
            // this is hugely noisy!
            //if (log.isWarnEnabled()) log.warn("Unrecognised attribute " + propName + ".  Please check your configuration.  Ignoring!!");
         }
         catch (Exception e) {
            throw new CacheConfigurationException("Unable to invoke setter " + setter + " on " + objectClass, e);
         }

         boolean setterFound = false;
         // if we get here, we could not find a String or Element setter.
         for (Method m : objectClass.getMethods()) {
            if (setter.equals(m.getName()) || fluentSetter.equals(m.getName())) {
               Class<?> paramTypes[] = m.getParameterTypes();
               if (paramTypes.length != 1) {
                  log.tracef("Rejecting setter %s on class %s due to incorrect number of parameters", m, objectClass);
                  continue; // try another param with the same name.
               }

               Class<?> parameterType = paramTypes[0];
               PropertyEditor editor = PropertyEditorManager.findEditor(parameterType);
               if (editor == null) {
                  throw new CacheConfigurationException("Couldn't find a property editor for parameter type " + parameterType);
               }

               editor.setAsText((String) attribs.get(propName));

               Object parameter = editor.getValue();
               //if (log.isDebugEnabled()) log.debug("Invoking setter method: " + setter + " with parameter \"" + parameter + "\" of type " + parameter.getClass());

               try {
                  m.invoke(target, parameter);
                  setterFound = true;
                  break;
               }
               catch (Exception e) {
                  throw new CacheConfigurationException("Unable to invoke setter " + setter + " on " + objectClass, e);
               }
            }
         }
         // Skip hot rod properties ...
         if (!setterFound && failOnMissingSetter && !propName.startsWith("infinispan.client.hotrod"))
            throw new CacheConfigurationException("Couldn't find a setter named [" + setter + "] which takes a single parameter, for parameter " + propName + " on class [" + objectClass + "]");
      }
   }

   public static Properties extractProperties(Element source) {
      TypedProperties p = new TypedProperties();
      NodeList list = source.getElementsByTagName("property");
      if (list == null) return null;
      // loop through attributes
      for (int loop = 0; loop < list.getLength(); loop++) {
         Node node = list.item(loop);
         if (node.getNodeType() != Node.ELEMENT_NODE) continue;

         // for each element (attribute) ...
         Element element = (Element) node;
         String name = element.getAttribute("name");
         String valueStr = element.getAttribute("value");

         if (valueStr.length() > 0) {
            valueStr = valueStr.trim();
            valueStr = StringPropertyReplacer.replaceProperties(valueStr);
            p.put(name, valueStr);
         }
      }
      return p.isEmpty() ? null : p;
   }

   public static String toString(Element e) {
      try {
         TransformerFactory tfactory = TransformerFactory.newInstance();
         Transformer xform = tfactory.newTransformer();
         Source src = new DOMSource(e);
         java.io.StringWriter writer = new StringWriter();
         Result result = new javax.xml.transform.stream.StreamResult(writer);
         xform.transform(src, result);
         return writer.toString();
      }
      catch (Exception ex) {
         return "Unable to convert to string: " + ex.toString();
      }
   }
}
