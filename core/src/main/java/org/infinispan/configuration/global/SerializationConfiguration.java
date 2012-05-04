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
package org.infinispan.configuration.global;

import java.util.Map;

import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.jboss.marshalling.ClassResolver;

public class SerializationConfiguration {

   private final Marshaller marshaller;
   private final short version;
   private final Map<Integer, AdvancedExternalizer<?>> advancedExternalizers;
   private final ClassResolver classResolver;
   
   SerializationConfiguration(Marshaller marshaller, short version,
         Map<Integer, AdvancedExternalizer<?>> advancedExternalizers,
         ClassResolver classResolver) {
      this.marshaller = marshaller;
      this.version = version;
      this.advancedExternalizers = advancedExternalizers;
      this.classResolver = classResolver;
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public short version() {
      return version;
   }
   
   public Map<Integer, AdvancedExternalizer<?>> advancedExternalizers() {
      return advancedExternalizers;
   }

   public ClassResolver classResolver() {
      return classResolver;
   }

   @Override
   public String toString() {
      return "SerializationConfiguration{" +
            "advancedExternalizers=" + advancedExternalizers +
            ", marshaller=" + marshaller +
            ", version=" + version +
            ", classResolver=" + classResolver +
            '}';
   }

}