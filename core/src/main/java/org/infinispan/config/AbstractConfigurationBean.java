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
package org.infinispan.config;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/**
 * Base superclass of Cache configuration classes that expose some properties that can be changed after the cache is
 * started.
 *
 * @author <a href="brian.stansberry@jboss.com">Brian Stansberry</a>
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractConfigurationBean implements CloneableConfigurationComponent, JAXBUnmarshallable {
   private static final long serialVersionUID = 4879873994727821938L;
   private static final Log log = LogFactory.getLog(AbstractConfigurationBean.class);
   private boolean accessible;
   protected Set<String> overriddenConfigurationElements = new HashSet<String>(4);

   protected AbstractConfigurationBean() {}
   

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
   protected static TypedProperties toTypedProperties(Properties p) {
      return TypedProperties.toTypedProperties(p);
   }

   protected static TypedProperties toTypedProperties(String s) {
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
         log.fieldNotFound(fieldName);
      }
      finally {
         accessible = false;
      }

      // now mark this as overridden
      overriddenConfigurationElements.add(fieldName);
   }

   @Override
   public CloneableConfigurationComponent clone() throws CloneNotSupportedException {
      AbstractConfigurationBean dolly = (AbstractConfigurationBean) super.clone();
      dolly.overriddenConfigurationElements = new HashSet<String>(this.overriddenConfigurationElements);
      return dolly;
   }

   @Override
   public void willUnmarshall(Object parent) {
      // default no-op
   }
}
