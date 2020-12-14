package org.infinispan.configuration.parsing;

import static org.infinispan.util.logging.Log.CONFIG;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.BeanUtils;
import org.w3c.dom.Element;

/**
 * A simple XML utility class for reading configuration elements.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class XmlConfigHelper {
   @SuppressWarnings({"rawtypes", "unchecked"})
   public static Object valueConverter(Class klass, String value) {
      if (klass == Character.class) {
         if (value.length() == 1) {
            return value.charAt(0);
         }
      } else if (klass == Byte.class) {
         return Byte.valueOf(value);
      } else if (klass == Short.class) {
         return Short.valueOf(value);
      } else if (klass == Integer.class) {
         return Integer.valueOf(value);
      } else if (klass == Long.class) {
         return Long.valueOf(value);
      } else if (klass == Boolean.class) {
         return Boolean.valueOf(value);
      } else if (klass == String.class) {
         return value;
      } else if (klass == char[].class) {
         return value.toCharArray();
      } else if (klass == Float.class) {
         return Float.valueOf(value);
      } else if (klass == Double.class) {
         return Double.valueOf(value);
      } else if (klass == BigDecimal.class) {
         return new BigDecimal(value);
      } else if (klass == BigInteger.class) {
         return new BigInteger(value);
      } else if (klass == File.class) {
         return new File(value);
      } else if (klass.isEnum()) {
         return Enum.valueOf(klass, value);
      } else if (klass == Properties.class) {
         try {
            Properties props = new Properties();
            props.load(new ByteArrayInputStream(value.getBytes()));
            return props;
         } catch (IOException e) {
            throw new CacheConfigurationException("Failed to load Properties from: " + value, e);
         }
      }

      throw new CacheConfigurationException("Cannot convert " + value + " to type " + klass.getName());
   }

   public static Map<Object, Object> setAttributes(AttributeSet attributes, Map<?, ?> attribs, boolean isXmlAttribs, boolean failOnMissingAttribute) {
      Map<Object, Object> ignoredAttribs = new HashMap<>();
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
      Map<Object, Object> ignoredAttribs = new HashMap<>();
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
                  CONFIG.tracef("Rejecting setter %s on class %s due to incorrect number of parameters", m, objectClass);
                  continue; // try another param with the same name.
               }

               Class<?> parameterType = paramTypes[0];

               if (parameterType.equals(Class.class)) {
                  CONFIG.tracef("Rejecting setter %s on class %s due to class parameter is type class", m, objectClass);
                   continue; // try another param with the same name.
               }

               Object parameter = valueConverter(parameterType, (String) attribs.get(propName));

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
         CONFIG.unrecognizedAttribute((String) propName);
      }
   }

}
