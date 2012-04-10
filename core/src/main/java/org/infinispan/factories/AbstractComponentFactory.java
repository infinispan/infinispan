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
package org.infinispan.factories;

import org.infinispan.config.ConfigurationException;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Factory that creates components used internally within Infinispan, and also wires dependencies into the components.
 * <p/>
 * The {@link InternalCacheFactory} is a special subclass of this, which bootstraps the construction of other
 * components. When this class is loaded, it maintains a static list of known default factories for known components,
 * which it then delegates to, when actually performing the construction.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see Inject
 * @see ComponentRegistry
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public abstract class AbstractComponentFactory {
   protected GlobalComponentRegistry globalComponentRegistry;
   protected GlobalConfiguration globalConfiguration;

   /**
    * Constructs a new ComponentFactory.
    */
   public AbstractComponentFactory() {
   }

   @Inject
   private void injectGlobalDependencies(GlobalConfiguration globalConfiguration, GlobalComponentRegistry globalComponentRegistry) {
      this.globalComponentRegistry = globalComponentRegistry;
      this.globalConfiguration = globalConfiguration;
   }

   /**
    * Constructs a component.
    *
    * @param componentType type of component
    * @return a component
    */
   public abstract <T> T construct(Class<T> componentType);

   protected void assertTypeConstructable(Class<?> requestedType, Class<?>... ableToConstruct) {
      boolean canConstruct = false;
      for (Class<?> c : ableToConstruct) {
         canConstruct = canConstruct || requestedType.isAssignableFrom(c);
      }
      if (!canConstruct) throw new ConfigurationException("Don't know how to construct " + requestedType);
   }

}
