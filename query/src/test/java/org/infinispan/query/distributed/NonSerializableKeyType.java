/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
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
package org.infinispan.query.distributed;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.marshall.Externalizer;
import org.infinispan.marshall.SerializeWith;
import org.infinispan.query.Transformable;
import org.infinispan.query.Transformer;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
@SerializeWith(NonSerializableKeyType.CustomExternalizer.class)
@Transformable(transformer = NonSerializableKeyType.CustomTransformer.class)
public class NonSerializableKeyType {

   public final String keyValue;

   public NonSerializableKeyType(final String keyValue) {
      this.keyValue = keyValue;
   }

   @Override
   public int hashCode() {
      return keyValue.hashCode();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      NonSerializableKeyType other = (NonSerializableKeyType) obj;
      if (keyValue == null) {
         if (other.keyValue != null)
            return false;
      } else if (!keyValue.equals(other.keyValue))
         return false;
      return true;
   }

   public static class CustomExternalizer implements Externalizer<NonSerializableKeyType> {
      @Override
      public void writeObject(ObjectOutput output, NonSerializableKeyType object) throws IOException {
         output.writeUTF(object.keyValue);
      }

      @Override
      public NonSerializableKeyType readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new NonSerializableKeyType(input.readUTF());
      }
   }

   public static class CustomTransformer implements Transformer {
      @Override
      public Object fromString(String s) {
         return new NonSerializableKeyType(s);
      }

      @Override
      public String toString(Object customType) {
         return ((NonSerializableKeyType)customType).keyValue;
      }
   }

}
