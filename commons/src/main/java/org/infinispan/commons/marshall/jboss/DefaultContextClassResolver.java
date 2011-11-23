/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.commons.marshall.jboss;

import org.jboss.marshalling.ContextClassResolver;

/**
 * This class refines <code>ContextClassLoader</code> to add a default class loader
 * in case the context class loader is <code>null</code>.
 *
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 4.2
 */
public class DefaultContextClassResolver extends ContextClassResolver {

   private ClassLoader defaultClassLoader;

   public DefaultContextClassResolver(ClassLoader defaultClassLoader) {
      this.defaultClassLoader = defaultClassLoader;
   }

   @Override
   protected ClassLoader getClassLoader() {
      ClassLoader loader = super.getClassLoader();
      return loader != null ? loader : defaultClassLoader;
   }
}
