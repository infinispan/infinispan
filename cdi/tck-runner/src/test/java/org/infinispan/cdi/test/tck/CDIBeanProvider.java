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
package org.infinispan.cdi.test.tck;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;

import javax.cache.annotation.BeanProvider;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.UnsatisfiedResolutionException;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import java.util.Set;

/**
 * The {@link BeanProvider} implementation. This bean provider is used to provide the beans needed for the annotation
 * tests.
 *
 * @auhor Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CDIBeanProvider implements BeanProvider {

   private final BeanManager beanManager;

   public CDIBeanProvider() {
      final WeldContainer weldContainer = new Weld().initialize();
      beanManager = weldContainer.getBeanManager();
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> T getBeanByType(Class<T> cls) {
      if (cls == null) {
         throw new NullPointerException("cls parameter cannot be null");
      }

      final CreationalContext<?> context = beanManager.createCreationalContext(null);
      final Set<Bean<?>> beans = beanManager.getBeans(cls);
      if (!beans.isEmpty()) {
         final Bean<?> bean = beanManager.resolve(beans);
         return (T) beanManager.getReference(bean, cls, context);
      }
      throw new UnsatisfiedResolutionException("There is no bean with type '" + cls + "'");
   }
}
