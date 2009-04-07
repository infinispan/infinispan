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

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.util.TypedProperties;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
   //   private transient CacheSPI cache; // back-reference to test whether the cache is running.
   //   private transient ComponentRegistry cr;
   // a workaround to get over immutability checks
   private boolean accessible;
   protected List<String> overriddenConfigurationElements = new LinkedList<String>();

   protected AbstractConfigurationBean() {
   }

//   public void passCacheToChildConfig(AbstractConfigurationBean child) {
//      if (child != null) {
//         child.setCache(cache);
//      }
//   }

//   protected void addChildConfig(AbstractConfigurationBean child) {
//      if (child != null) children.add(child);
//      if (child != null && children.add(child))
//         child.setCache(cache);
//   }

//   protected void addChildConfigs(Collection<? extends AbstractConfigurationBean> toAdd) {
//      if (toAdd != null) {
//         for (AbstractConfigurationBean child : toAdd)
//            addChildConfig(child);
//      }
//   }
//
//   protected void removeChildConfig(AbstractConfigurationBean child) {
//      children.remove(child);
//   }

//   protected void removeChildConfigs(Collection<? extends AbstractConfigurationBean> toRemove) {
//      if (toRemove != null) {
//         for (AbstractConfigurationBean child : toRemove)
//            removeChildConfig(child);
//      }
//   }
//
//   protected void replaceChildConfig(AbstractConfigurationBean oldConfig, AbstractConfigurationBean newConfig) {
//      removeChildConfig(oldConfig);
//      addChildConfig(newConfig);
//   }

//   protected void replaceChildConfigs(Collection<? extends AbstractConfigurationBean> oldConfigs,
//                                      Collection<? extends AbstractConfigurationBean> newConfigs) {
//      synchronized (children) {
//         removeChildConfigs(oldConfigs);
//         addChildConfigs(newConfigs);
//      }
//   }

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

//   public void setCache(CacheSPI cache) {
//      this.cache = cache;
//      synchronized (children) {
//         for (AbstractConfigurationBean child : children) {
//            child.setCache(cache);
//         }
//      }
//   }

//   @Start
//   private void start() {
//      setCache(cr.getComponent(CacheSPI.class));
//   }

   @Override
   public CloneableConfigurationComponent clone() throws CloneNotSupportedException {
      AbstractConfigurationBean c = (AbstractConfigurationBean) super.clone();
//      c.setCache(null);
      return c;
   }
}
