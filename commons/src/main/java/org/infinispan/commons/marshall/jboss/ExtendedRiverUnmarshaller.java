/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.commons.marshall.jboss;

import org.infinispan.api.marshall.StreamingMarshaller;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.reflect.SerializableClassRegistry;
import org.jboss.marshalling.river.RiverMarshallerFactory;
import org.jboss.marshalling.river.RiverUnmarshaller;

/**
 * An extended {@link RiverUnmarshaller} that allows Infinispan {@link StreamingMarshaller}
 * instances to travel down the stack to potential externalizer implementations
 * that might need it, such as {@link org.infinispan.commons.marshall.MarshalledValue.Externalizer}
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class ExtendedRiverUnmarshaller extends RiverUnmarshaller {

   private StreamingMarshaller infinispanMarshaller;

   protected ExtendedRiverUnmarshaller(RiverMarshallerFactory factory,
         SerializableClassRegistry registry, MarshallingConfiguration cfg) {
      super(factory, registry, cfg);
   }

   public StreamingMarshaller getInfinispanMarshaller() {
      return infinispanMarshaller;
   }

   public void setInfinispanMarshaller(StreamingMarshaller infinispanMarshaller) {
      this.infinispanMarshaller = infinispanMarshaller;
   }

}
