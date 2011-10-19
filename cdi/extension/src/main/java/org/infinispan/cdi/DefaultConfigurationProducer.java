/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.cdi;

import org.infinispan.cdi.util.logging.Log;
import org.infinispan.config.Configuration;
import org.infinispan.util.logging.LogFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;

/**
 * <p>The default configuration producer.</p>
 *
 * <p>The default configuration is used by the default cache manager. The default configuration can be overridden by
 * creating a producer which produces the new default configuration. The new default configuration produced must be
 * qualified by {@link OverrideDefault}.</p>
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultConfigurationProducer {

   private static final Log log = LogFactory.getLog(DefaultConfigurationProducer.class, Log.class);

   /**
    * Produces the default configuration.
    *
    * @param providedDefaultConfiguration the provided default configuration.
    * @return the default configuration used by the default cache manager.
    */
   @Produces
   @Default
   @ConfigureCache
   @ApplicationScoped
   public Configuration getDefaultCacheConfiguration(@OverrideDefault Instance<Configuration> providedDefaultConfiguration) {
      if (!providedDefaultConfiguration.isUnsatisfied()) {
         if (log.isTraceEnabled()) {
            log.tracef("Default cache manager configuration overridden by '%s'", providedDefaultConfiguration);
         }
         return providedDefaultConfiguration.get();
      }
      return new Configuration();
   }
}
