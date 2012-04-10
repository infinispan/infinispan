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

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * Adds named cache specific features to the {@link org.infinispan.config.AbstractConfigurationBean}
 * .
 * 
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractNamedCacheConfigurationBean extends AbstractConfigurationBean {

   private static final long serialVersionUID = -3838074220419703543L;
   
   protected ComponentRegistry cr;

   @Inject
   public void inject(ComponentRegistry cr) {
      this.cr = cr;
   }

   @Override
   protected boolean hasComponentStarted() {
      return cr != null && cr.getStatus() != null && cr.getStatus() == ComponentStatus.RUNNING;
   }

   @Override
   public AbstractNamedCacheConfigurationBean clone() throws CloneNotSupportedException {
      AbstractNamedCacheConfigurationBean dolly = (AbstractNamedCacheConfigurationBean) super.clone();
      if (cr != null)
         dolly.cr = (ComponentRegistry) cr.clone();
      return dolly;
   }

   static class InjectComponentRegistryVisitor extends AbstractConfigurationBeanVisitor {

      private final ComponentRegistry registry;
      
      public InjectComponentRegistryVisitor(ComponentRegistry registry) {
         super();
         this.registry = registry;
      }
      @Override
      public void defaultVisit(AbstractConfigurationBean c) {
         if (c instanceof AbstractNamedCacheConfigurationBean) {
            ((AbstractNamedCacheConfigurationBean) c).cr = registry;
         }
      }
   }
}
