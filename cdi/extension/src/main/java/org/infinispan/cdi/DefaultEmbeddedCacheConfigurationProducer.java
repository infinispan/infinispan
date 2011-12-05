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
import org.jboss.solder.bean.defaultbean.DefaultBean;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;

/**
 * <p>The default embedded cache {@link Configuration} producer.</p>
 *
 * <p>The default embedded cache configuration can be overridden by creating a producer which produces the new default
 * configuration. The configuration produced must have the {@link ApplicationScoped} and the
 * {@link javax.enterprise.inject.Default Default} qualifier.</p>
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultEmbeddedCacheConfigurationProducer {

   private static final Log log = LogFactory.getLog(DefaultEmbeddedCacheConfigurationProducer.class, Log.class);

   /**
    * Produces the default embedded cache configuration.
    *
    * @param providedDefaultEmbeddedCacheConfiguration the provided default embedded cache configuration.
    * @return the default embedded cache configuration.
    */
   @Produces
   @ConfigureCache
   @ApplicationScoped
   @DefaultBean(Configuration.class)
   public Configuration getDefaultEmbeddedCacheConfiguration(@OverrideDefault Instance<Configuration> providedDefaultEmbeddedCacheConfiguration) {
      if (!providedDefaultEmbeddedCacheConfiguration.isUnsatisfied()) {
         log.tracef("Default embedded cache configuration overridden by '%s'", providedDefaultEmbeddedCacheConfiguration);
         return providedDefaultEmbeddedCacheConfiguration.get();
      }
      return new Configuration();
   }
}
