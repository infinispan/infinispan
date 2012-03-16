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
package org.infinispan.config.parsing;

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.GlobalConfiguration;

import java.util.Map;

/**
 * Implementations of this interface are responsible for parsing XML configuration files.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface XmlConfigurationParser {
   /**
    * Parses the default template configuration.
    *
    * @return a configuration instance representing the "default" block in the configuration file
    * @throws ConfigurationException if there is a problem parsing the configuration XML
    */
   Configuration parseDefaultConfiguration() throws ConfigurationException;

   /**
    * Parses and retrieves configuration overrides for named caches.
    *
    * @return a Map of Configuration overrides keyed on cache name
    * @throws ConfigurationException if there is a problem parsing the configuration XML
    */
   Map<String, Configuration> parseNamedConfigurations() throws ConfigurationException;

   /**
    * GlobalConfiguration would also have a reference to the template default configuration, accessible via {@link
    * org.infinispan.config.GlobalConfiguration#getDefaultConfiguration()}
    * <p/>
    * This is typically used to configure a {@link org.infinispan.manager.DefaultCacheManager}
    *
    * @return a GlobalConfiguration as parsed from the configuration file.
    */
   GlobalConfiguration parseGlobalConfiguration();
}
