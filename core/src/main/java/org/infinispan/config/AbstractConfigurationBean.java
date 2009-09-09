/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.config;

import org.infinispan.CacheException;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Base superclass of Cache configuration classes that expose some properties that can be changed after the cache is
 * started.
 *
 * @author <a href="brian.stansberry@jboss.com">Brian Stansberry</a>
 * @see #testImmutability(String)
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractConfigurationBean implements CloneableConfigurationComponent {
   private static final long serialVersionUID = 4879873994727821938L;
   protected static final TypedProperties EMPTY_PROPERTIES = new TypedProperties();
   protected transient Log log = LogFactory.getLog(getClass());  
   private boolean accessible;
   protected List<String> overriddenConfigurationElements = new LinkedList<String>();

   protected AbstractConfigurationBean() {
   }
   
   public void accept(ConfigurationBeanVisitor v){
       v.visit(this);      
   }

   /**
    * Safely converts a String to upper case.
    *
    * @param s string to convert
    * @return the string in upper case, or null if s is null.
    */
   protected String uc(String s) {
      return s == null ? null : s.toUpperCase(Locale.ENGLISH);
   }

   /**
    * Converts a given {@link Properties} instance to an instance of {@link TypedProperties}
    *
    * @param p properties to convert
    * @return TypedProperties instance
    */
   protected TypedProperties toTypedProperties(Properties p) {
      return TypedProperties.toTypedProperties(p);
   }

   protected TypedProperties toTypedProperties(String s) {
      TypedProperties tp = new TypedProperties();
      if (s != null && s.trim().length() > 0) {
         InputStream stream = new ByteArrayInputStream(s.getBytes());
         try {
            tp.load(stream);
         } catch (IOException e) {
            throw new ConfigurationException("Unable to parse properties string " + s, e);
         }
      }
      return tp;
   }

   /**
    * Tests whether the component this configuration bean intents to configure has already started.
    *
    * @return true if the component has started; false otherwise.
    */
   protected abstract boolean hasComponentStarted();

   /**
    * Checks field modifications via setters
    *
    * @param fieldName
    */
   protected void testImmutability(String fieldName) {
      try {
         if (!accessible && hasComponentStarted() && !getClass().getDeclaredField(fieldName).isAnnotationPresent(Dynamic.class)) {
            throw new ConfigurationException("Attempted to modify a non-Dynamic configuration element [" + fieldName + "] after the component has started!");
         }
      }
      catch (NoSuchFieldException e) {
         log.warn("Field " + fieldName + " not found!!");
      }
      finally {
         accessible = false;
      }

      // now mark this as overridden
      overriddenConfigurationElements.add(fieldName);
   }

   public void applyOverrides(AbstractConfigurationBean overrides) {
      //does this component have overridden fields?
      for (String overridenField : overrides.overriddenConfigurationElements) {
         try {
            ReflectionUtil.setValue(this, overridenField, ReflectionUtil.getValue(overrides,overridenField));
         } catch (Exception e1) {
            throw new CacheException("Could not apply value for field " + overridenField
                     + " from instance " + overrides + " on instance " + this, e1);
         }
      }

      // then recurse into field of this component...
      List<Field> fields = ReflectionUtil.getFields(overrides.getClass(),AbstractConfigurationBean.class);
      for (Field field : fields) {
         if (AbstractConfigurationBean.class.isAssignableFrom(field.getType())) {
            AbstractConfigurationBean fieldValueOverrides = null;
            AbstractConfigurationBean fieldValueThis = null;
            try {
               field.setAccessible(true);
               fieldValueOverrides = (AbstractConfigurationBean) field.get(overrides);
               fieldValueThis = (AbstractConfigurationBean) field.get(this);
               if (fieldValueThis == null && fieldValueOverrides != null){
                  field.set(this, fieldValueOverrides);
               }
               else if(fieldValueOverrides != null && fieldValueThis!=null){
                  fieldValueThis.applyOverrides(fieldValueOverrides);
               }
            } catch (IllegalAccessException e) {
               String s = "Could not apply override for field " + field + " in class " + overrides;
               log.error(s, e);
               throw new CacheException(s, e);
            }
         }
      }

      //and don't forget to recurse into collections of components...
      fields = ReflectionUtil.getFields(overrides.getClass(), Collection.class);
      for (Field field : fields) {
         Type genericType = field.getGenericType();
         if (genericType instanceof ParameterizedType) {
            ParameterizedType aType = (ParameterizedType) genericType;
            Type[] fieldArgTypes = aType.getActualTypeArguments();
            for (Type fieldArgType : fieldArgTypes) {
               Class<?> fieldArgClass = (Class<?>) fieldArgType;
               if (!(fieldArgClass.isPrimitive() || fieldArgClass.equals(String.class))) {
                  try {
                     field.setAccessible(true);
                     Collection<Object> c = (Collection<Object>) field.get(this);
                     Collection<Object> c2 = (Collection<Object>) field.get(overrides);
                     if (c.isEmpty() && !c2.isEmpty()) {
                        c.addAll(c2);
                     } else if (!c.isEmpty() && !c2.isEmpty()) {
                        Iterator<?> i = c.iterator();
                        Iterator<?> i2 = c2.iterator();
                        for (; i.hasNext() && i2.hasNext();) {
                           Object nextThis = i.next();
                           Object nextOverrides = i2.next();
                           if (AbstractConfigurationBean.class.isAssignableFrom(nextThis.getClass())
                                    && AbstractConfigurationBean.class.isAssignableFrom(nextOverrides.getClass())) {
                              ((AbstractConfigurationBean) nextThis).applyOverrides((AbstractConfigurationBean) nextOverrides);
                           }
                        }
                        while (i2.hasNext()) {
                           c.add(i2.next());
                        }
                     }
                  } catch (IllegalAccessException e) {
                     String s = "Could not apply override for field " + field + " in class " + overrides.getClass();
                     log.error(s, e);
                     throw new CacheException(s, e);
                  }
               }
            }
         }
      }
   }

   @Override
   public CloneableConfigurationComponent clone() throws CloneNotSupportedException {
      AbstractConfigurationBean c = (AbstractConfigurationBean) super.clone();
//      c.setCache(null);
      return c;
   }
}
