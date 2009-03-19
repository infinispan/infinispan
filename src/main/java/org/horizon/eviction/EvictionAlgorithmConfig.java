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
package org.horizon.eviction;

import org.horizon.config.CloneableConfigurationComponent;
import org.horizon.config.ConfigurationException;

/**
 * An interface used to configure an eviction algorithm.  Replaces the deprecated {@link EvictionCacheConfig}.
 * <p/>
 * In its most basic form, it is implemented by {@link org.horizon.eviction.algorithms.BaseEvictionAlgorithmConfig}, but
 * more specific eviction policies may subclass {@link org.horizon.eviction.algorithms.BaseEvictionAlgorithmConfig} or
 * re-implement this interface to provide access to more config variables.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 1.0
 */
public interface EvictionAlgorithmConfig extends CloneableConfigurationComponent {
   /**
    * Gets the class name of the {@link org.horizon.eviction.EvictionAlgorithm} implementation this object will
    * configure.
    *
    * @return fully qualified class name
    */
   String getEvictionAlgorithmClassName();

   /**
    * Validate the configuration. Will be called after any configuration properties are set.
    *
    * @throws org.horizon.config.ConfigurationException
    *          if any values for the configuration properties are invalid
    */
   void validate() throws ConfigurationException;

   /**
    * Resets the values to their defaults.
    */
   void reset();

   /**
    * @return a clone of the EvictionAlgorithmConfig.
    */
   EvictionAlgorithmConfig clone();
}
