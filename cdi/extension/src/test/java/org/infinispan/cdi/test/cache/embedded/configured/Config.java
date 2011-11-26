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
package org.infinispan.cdi.test.cache.embedded.configured;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.config.Configuration;

import javax.enterprise.inject.Produces;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {
   /**
    * <p>Configures a "tiny" cache (with a very low number of entries), and associates it with the qualifier {@link
    * Tiny}.</p>
    *
    * <p>This will use the default cache container.</p>
    */
   @Produces
   @ConfigureCache("tiny")
   @Tiny
   public Configuration tinyConfiguration() {
      return new Configuration().fluent()
            .eviction().maxEntries(1)
            .build();
   }

   /**
    * <p>Configures a "small" cache (with a pretty low number of entries), and associates it with the qualifier {@link
    * Small}.</p>
    *
    * <p>This will use the default cache container.</p>
    */
   @Produces
   @ConfigureCache("small")
   @Small
   public Configuration smallConfiguration() {
      return new Configuration().fluent()
            .eviction().maxEntries(10)
            .build();
   }
}
