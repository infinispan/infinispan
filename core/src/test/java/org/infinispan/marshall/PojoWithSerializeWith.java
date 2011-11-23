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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.SerializeWith;

/**
 * A test pojo that is marshalled using Infinispan's
 * {@link org.infinispan.marshall.Externalizer} which is annotated with
 * {@link SerializeWith}
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@SerializeWith(PojoWithSerializeWith.Externalizer.class)
public class PojoWithSerializeWith {

   final PojoWithAttributes pojo;

   public PojoWithSerializeWith(int age, String key) {
      this.pojo = new PojoWithAttributes(age, key);
   }

   public PojoWithSerializeWith(PojoWithAttributes pojo) {
      this.pojo = pojo;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PojoWithSerializeWith that = (PojoWithSerializeWith) o;

      if (pojo != null ? !pojo.equals(that.pojo) : that.pojo != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      return pojo != null ? pojo.hashCode() : 0;
   }

   public static class Externalizer implements org.infinispan.api.marshall.Externalizer<PojoWithSerializeWith> {
      @Override
      public void writeObject(ObjectOutput output, PojoWithSerializeWith object) throws IOException {
         PojoWithAttributes.writeObject(output, object.pojo);
      }

      @Override
      public PojoWithSerializeWith readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new PojoWithSerializeWith(PojoWithAttributes.readObject(input));
      }
   }
}
