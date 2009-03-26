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
package org.horizon.config;

import org.horizon.config.parsing.XmlConfigHelper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * A configuration component where the implementation class can be specified, and comes with its own set of properties.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public abstract class PluggableConfigurationComponent extends AbstractNamedCacheConfigurationBean {
   protected Properties properties = EMPTY_PROPERTIES;

   public Properties getProperties() {
      return properties;
   }

   public void setProperties(Properties properties) {
      testImmutability("properties");
      this.properties = properties;
   }

   public void setProperties(String properties) throws IOException {
      if (properties == null) return;

      testImmutability("properties");
      // JBCACHE-531: escape all backslash characters
      // replace any "\" that is not preceded by a backslash with "\\"
      properties = XmlConfigHelper.escapeBackslashes(properties);
      ByteArrayInputStream is = new ByteArrayInputStream(properties.trim().getBytes("ISO8859_1"));
      this.properties = new Properties();
      this.properties.load(is);
      is.close();
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PluggableConfigurationComponent that = (PluggableConfigurationComponent) o;

      if (properties != null ? !properties.equals(that.properties) : that.properties != null) return false;

      return true;
   }

   public int hashCode() {
      return (properties != null ? properties.hashCode() : 0);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName()  +
            ", properties=" + properties + "}";
   }

   @Override
   public PluggableConfigurationComponent clone() throws CloneNotSupportedException {
      PluggableConfigurationComponent clone = (PluggableConfigurationComponent) super.clone();
      if (properties != null) clone.properties = (Properties) properties.clone();
      return clone;
   }
}
