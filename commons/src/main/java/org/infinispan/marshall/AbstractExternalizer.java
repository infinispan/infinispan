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

package org.infinispan.marshall;

/**
 * Base class for {@link AdvancedExternalizer} implementations that offers default
 * implementations for some of its methods. In particular, this base class
 * offers a default implementation for {@link org.infinispan.marshall.AdvancedExternalizer#getId()}
 * that returns null which is particularly useful for advanced externalizers
 * whose id will be provided by XML or programmatic configuration rather than
 * the externalizer implementation itself.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public abstract class AbstractExternalizer<T> implements AdvancedExternalizer<T> {

   @Override
   public Integer getId() {
      return null;
   }

}
