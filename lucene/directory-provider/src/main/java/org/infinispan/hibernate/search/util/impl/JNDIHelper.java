package org.infinispan.hibernate.search.util.impl;

import javax.naming.Context;
import java.util.HashSet;
import java.util.Properties;

/**
 * Helper class for creating an JNDI {@code InitialContext}.
 *
 * @author Hardy Ferentschik
 */
public class JNDIHelper {

   public static final String HIBERNATE_JNDI_PREFIX = "hibernate.jndi.";

   private JNDIHelper() {
   }

   public static Properties getJndiProperties(Properties properties, String prefix) {

      HashSet<String> specialProps = new HashSet<String>();
      specialProps.add(prefix + "class");
      specialProps.add(prefix + "url");

      Properties result = addJNDIProperties(properties, prefix, specialProps);

      handleSpecialPropertyTranslation(properties, prefix + "class", result, Context.INITIAL_CONTEXT_FACTORY);
      handleSpecialPropertyTranslation(properties, prefix + "url", result, Context.PROVIDER_URL);

      return result;
   }

   /**
    * Creates a new {@code Properties} instance with all properties from {@code properties} which start with the given
    *
    * @param properties   the original properties
    * @param prefix       the prefix indicating JNDI specific properties
    * @param specialProps a set of property names to ignore
    * @return Creates a new {@code Properties} instance with JNDI specific properties
    * @{code prefix}. In the new instance the prefix is removed. If a property matches a value in {@code specialProps}
    * it gets ignored.
    */
   private static Properties addJNDIProperties(Properties properties, String prefix, HashSet<String> specialProps) {
      Properties result = new Properties();
      for (Object property : properties.keySet()) {
         if (property instanceof String) {
            String s = (String) property;
            if (s.indexOf(prefix) > -1 && !specialProps.contains(s)) {
               result.setProperty(s.substring(prefix.length()), properties.getProperty(s));
            }
         }
      }
      return result;
   }

   private static void handleSpecialPropertyTranslation(Properties originalProperties, String oldKey, Properties newProperties, String newKey) {
      String value = originalProperties.getProperty(oldKey);
      // we want to be able to just use the defaults,
      // if JNDI environment properties are not supplied
      // so don't put null in anywhere
      if (value != null) {
         newProperties.put(newKey, value);
      }
   }
}
