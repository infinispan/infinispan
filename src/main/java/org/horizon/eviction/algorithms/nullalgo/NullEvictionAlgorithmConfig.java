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
package org.horizon.eviction.algorithms.nullalgo;

import org.horizon.config.AbstractNamedCacheConfigurationBean;
import org.horizon.config.ConfigurationException;
import org.horizon.eviction.EvictionAlgorithmConfig;

/**
 * Configuration class for {@link NullEvictionAlgorithm}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class NullEvictionAlgorithmConfig extends AbstractNamedCacheConfigurationBean implements EvictionAlgorithmConfig {
   private static final long serialVersionUID = -6591180473728241737L;

   /**
    * No-op
    */
   public void reset() {
      // no-op
   }

   public String getEvictionAlgorithmClassName() {
      return NullEvictionAlgorithm.class.getName();
   }

   /**
    * No-op
    */
   public void validate() throws ConfigurationException {
      // no-op
   }

   public NullEvictionAlgorithmConfig clone() {
      try {
         return (NullEvictionAlgorithmConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen", e);
      }
   }
}