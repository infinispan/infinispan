package org.infinispan.configuration.parsing;

import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.util.BeanUtils;
import org.infinispan.commons.util.Util;
import org.w3c.dom.Element;

/**
 * A simple XML utility class for reading configuration elements.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class XmlConfigHelper {

   public static Map<Object, Object> setAttributes(AttributeSet attributes, Map<?, ?> attribs, boolean isXmlAttribs, boolean failOnMissingAttribute) {
      Map<Object, Object> ignoredAttribs = new HashMap<>();
      for(Entry<?, ?> entry : attribs.entrySet()) {
         String name = NamingStrategy.KEBAB_CASE.convert((String) entry.getKey());
         if (attributes.contains(name)) {
            Attribute<Object> attribute = attributes.attribute(name);
            attribute.set(Util.fromString(attribute.getAttributeDefinition().getType(), (String) entry.getValue()));
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

               Object parameter = Util.fromString(parameterType, (String) attribs.get(propName));

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
