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

package org.infinispan.marshall.jboss;

import org.jboss.marshalling.AbstractMarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.reflect.SerializableClassRegistry;
import org.jboss.marshalling.river.RiverMarshallerFactory;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * A JBoss Marshalling factory class for retrieving marshaller/unmarshaller
 * instances. The aim of this factory is to allow Infinispan to provide its own
 * JBoss Marshalling marshaller/unmarshaller extensions.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class JBossMarshallerFactory extends AbstractMarshallerFactory {

   private final SerializableClassRegistry registry;

   private final RiverMarshallerFactory factory;

   public JBossMarshallerFactory() {
      factory = (RiverMarshallerFactory) Marshalling.getMarshallerFactory(
            "river", Marshalling.class.getClassLoader());
      if (factory == null)
         throw new IllegalStateException(
            "River marshaller factory not found.  Verify that the JBoss Marshalling River jar archive is in the classpath.");

      registry = AccessController.doPrivileged(new PrivilegedAction<SerializableClassRegistry>() {
          public SerializableClassRegistry run() {
              return SerializableClassRegistry.getInstance();
          }
      });
   }

   @Override
   public ExtendedRiverUnmarshaller createUnmarshaller(MarshallingConfiguration configuration) throws IOException {
      return new ExtendedRiverUnmarshaller(factory, registry, configuration);
   }

   @Override
   public ExtendedRiverMarshaller createMarshaller(MarshallingConfiguration configuration) throws IOException {
      return new ExtendedRiverMarshaller(factory, registry, configuration);
   }

}
