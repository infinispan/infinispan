/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.util;

import java.util.Properties;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.config.TypedPropertiesAdapter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Type-aware properties.  Extends the JDK {@link Properties} class to provide accessors that convert values to certain
 * types, using default values if a conversion is not possible.
 * 
 *
 * @configRef name="Properties to add to the enclosing component."
 *
 * @author Manik Surtani
 * @since 4.0
 */
@XmlJavaTypeAdapter(TypedPropertiesAdapter.class)
@XmlType(name="properties")
public class TypedProperties extends Properties {

   /** The serialVersionUID */
   private static final long serialVersionUID = 3799321248100686287L;

   private static final Log log = LogFactory.getLog(TypedProperties.class);

   /**
    * Copy constructor
    *
    * @param p properties instance to from.  If null, then it is treated as an empty Properties instance.
    */
   public TypedProperties(Properties p) {
      if (p != null && !p.isEmpty()) putAll(p);
   }

   /**
    * Default constructor that returns an empty instance
    */
   public TypedProperties() {

   }

   /**
    * Factory method that converts a JDK {@link Properties} instance to an instance of TypedProperties, if needed.
    *
    * @param p properties to convert.
    * @return A TypedProperties object.  Returns an empty TypedProperties instance if p is null.
    */
   public static TypedProperties toTypedProperties(Properties p) {
      if (p instanceof TypedProperties) return (TypedProperties) p;
      return new TypedProperties(p);
   }

   public int getIntProperty(String key, int defaultValue) {
      return getIntProperty(key, defaultValue, false);
   }

   public int getIntProperty(String key, int defaultValue, boolean doStringReplace) {
      String value = getProperty(key);
      if (value == null) return defaultValue;
      value = value.trim();
      if (value.length() == 0) return defaultValue;

      if (doStringReplace)
         value = StringPropertyReplacer.replaceProperties(value);

      try {
         return Integer.parseInt(value);
      }
      catch (NumberFormatException nfe) {
         log.unableToConvertStringPropertyToInt(value, defaultValue);
         return defaultValue;
      }
   }

   public long getLongProperty(String key, long defaultValue) {
      return getLongProperty(key, defaultValue, false);
   }

   public long getLongProperty(String key, long defaultValue, boolean doStringReplace) {
      String value = getProperty(key);
      if (value == null) return defaultValue;
      value = value.trim();
      if (value.length() == 0) return defaultValue;

      if (doStringReplace)
         value = StringPropertyReplacer.replaceProperties(value);

      try {
         return Long.parseLong(value);
      }
      catch (NumberFormatException nfe) {
         log.unableToConvertStringPropertyToLong(value, defaultValue);
         return defaultValue;
      }
   }

   public boolean getBooleanProperty(String key, boolean defaultValue) {
      return getBooleanProperty(key, defaultValue, false);
   }

   public boolean getBooleanProperty(String key, boolean defaultValue, boolean doStringReplace) {
      String value = getProperty(key);
      if (value == null) return defaultValue;
      value = value.trim();
      if (value.length() == 0) return defaultValue;

      if (doStringReplace)
         value = StringPropertyReplacer.replaceProperties(value);

      try {
         return Boolean.parseBoolean(value);
      }
      catch (Exception e) {
         log.unableToConvertStringPropertyToBoolean(value, defaultValue);
         return defaultValue;
      }
   }

   /**
    * Get the property associated with the key, optionally applying string property replacement as defined in
    * {@link StringPropertyReplacer#replaceProperties} to the result.
    *
    * @param   key               the hashtable key.
    * @param   defaultValue      a default value.
    * @param   doStringReplace   boolean indicating whether to apply string property replacement
    * @return  the value in this property list with the specified key valu after optionally being inspected for String property replacement
    */
   public String getProperty(String key, String defaultValue, boolean doStringReplace) {
      if (doStringReplace)
         return StringPropertyReplacer.replaceProperties(getProperty(key, defaultValue));
      else
         return getProperty(key, defaultValue);
   }

   @Override
   public synchronized TypedProperties setProperty(String key, String value) {
      super.setProperty(key, value);
      return this;
   }

}
