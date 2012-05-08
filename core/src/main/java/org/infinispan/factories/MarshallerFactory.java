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

import org.infinispan.CacheException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.marshall.CacheMarshaller;
import org.infinispan.marshall.GlobalMarshaller;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.VersionAwareMarshaller;

import static org.infinispan.factories.KnownComponentNames.*;

/**
 * MarshallerFactory.
 *
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StreamingMarshaller.class, Marshaller.class})
public class MarshallerFactory extends NamedComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType, String componentName) {
      Object comp;
      if (componentName.equals(GLOBAL_MARSHALLER))
         comp = new GlobalMarshaller((VersionAwareMarshaller)
               globalConfiguration.serialization().marshaller());
      else if (componentName.equals(CACHE_MARSHALLER))
         comp = new CacheMarshaller(new VersionAwareMarshaller());
      else
         throw new CacheException("Don't know how to handle type " + componentType);

      try {
         return componentType.cast(comp);
      } catch (Exception e) {
         throw new CacheException("Problems casting bootstrap component " + comp.getClass() + " to type " + componentType, e);
      }
   }

}
