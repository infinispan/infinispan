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

import org.jboss.marshalling.Creator;
import org.jboss.marshalling.Externalize;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A test pojo that is marshalled using JBoss Marshalling's
 * {@link org.jboss.marshalling.Externalizer} which is annotated with
 * {@link Externalize}
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Externalize(PojoWithJBossExternalize.Externalizer.class)
public class PojoWithJBossExternalize {
   final PojoWithAttributes pojo;

   public PojoWithJBossExternalize(int age, String key) {
      this.pojo = new PojoWithAttributes(age, key);
   }

   PojoWithJBossExternalize(PojoWithAttributes pojo) {
      this.pojo = pojo;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PojoWithJBossExternalize that = (PojoWithJBossExternalize) o;

      if (pojo != null ? !pojo.equals(that.pojo) : that.pojo != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      return pojo != null ? pojo.hashCode() : 0;
   }

   public static class Externalizer implements org.jboss.marshalling.Externalizer {
      @Override
      public void writeExternal(Object subject, ObjectOutput output) throws IOException {
         PojoWithAttributes.writeObject(output, ((PojoWithJBossExternalize) subject).pojo);
      }

      @Override
      public Object createExternal(Class<?> subjectType, ObjectInput input, Creator defaultCreator) throws IOException, ClassNotFoundException {
         return new PojoWithJBossExternalize(PojoWithAttributes.readObject(input));
      }

      @Override
      public void readExternal(Object subject, ObjectInput input) throws IOException, ClassNotFoundException {
      }
   }
}
