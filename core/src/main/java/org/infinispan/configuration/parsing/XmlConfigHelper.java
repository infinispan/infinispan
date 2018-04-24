package org.infinispan.configuration.parsing;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.BeanUtils;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * A simple XML utility class for reading configuration elements
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class XmlConfigHelper {
   private static final Log log = LogFactory.getLog(XmlConfigHelper.class);

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

   public static Object valueConverter(@SuppressWarnings("rawtypes") Class klass, String value) {
      if (klass == Integer.class) {
         return Integer.valueOf(value);
      } else if (klass == Long.class) {
         return Long.valueOf(value);
      } else if (klass == Boolean.class) {
         return Boolean.valueOf(value);
      } else if (klass == String.class) {
         return value;
      } else if (klass == Float.class) {
         return Float.valueOf(value);
      } else if (klass == Double.class) {
         return Double.valueOf(value);
      } else if (klass .isEnum()) {
         return Enum.valueOf(klass, value);
      } else {
         throw new CacheConfigurationException("Cannot convert "+ value + " to type " + klass.getName());
      }
   }

   public static Map<Object, Object> setAttributes(AttributeSet attributes, Map<?, ?> attribs, boolean isXmlAttribs, boolean failOnMissingAttribute) {
      Map<Object, Object> ignoredAttribs = new HashMap<Object, Object>();
      for(Entry<?, ?> entry : attribs.entrySet()) {
         String name = (String) entry.getKey();
         if (attributes.contains(name)) {
            Attribute<Object> attribute = attributes.attribute(name);
            attribute.set(valueConverter(attribute.getAttributeDefinition().getType(), (String) entry.getValue()));
         } else if (failOnMissingAttribute) {
            throw new CacheConfigurationException("Couldn't find an attribute named [" + name + "] on attribute set [" + attributes.getName() + "]");
         } else {
            ignoredAttribs.put(name, entry.getValue());
         }
      }
      return ignoredAttribs;
   }

   public static Map<Object, Object> setValues(Object target, Map<?, ?> attribs, boolean isXmlAttribs, boolean failOnMissingSetter) {
      Class<?> objectClass = target.getClass();
      Map<Object, Object> ignoredAttribs = new HashMap<Object, Object>();
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

               if (parameterType.equals(Class.class)) {
                  log.tracef("Rejecting setter %s on class %s due to class parameter is type class", m, objectClass);
                   continue; // try another param with the same name.
               }
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
         if (!setterFound && !propName.startsWith("infinispan.client.hotrod"))
            if (failOnMissingSetter) {
               throw new CacheConfigurationException("Couldn't find a setter named [" + setter + "] which takes a single parameter, for parameter " + propName + " on class [" + objectClass + "]");
            } else {
               ignoredAttribs.put(propName, attribs.get(propName));
            }
      }
      return ignoredAttribs;
   }

   public static void showUnrecognizedAttributes(Map<Object, Object> attribs) {
      for(Object propName : attribs.keySet()) {
         log.unrecognizedAttribute((String) propName);
      }
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
