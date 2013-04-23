/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.spring.provider;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.infinispan.marshall.SerializeWith;
import org.springframework.cache.Cache.ValueWrapper;

/**
 * A placeholder value for storing {@literal null} in a cache.
 * 
 * @author <a href="mailto:olaf.bergner@gmx.de">Olaf Bergner</a>
 * @since 5.3
 */
@SerializeWith(NullValue.Externalizer.class)
final class NullValue implements ValueWrapper, Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = 6104836300055942197L;

   static final NullValue NULL = new NullValue();

   /**
    * Always returns {@literal null}.
    * 
    * @return {@literal null}
    * @see org.springframework.cache.Cache.ValueWrapper#get()
    */
   @Override
   public Object get() {
      return null;
   }

   /**
    * Create a new NullValue.
    * 
    */
   private NullValue() {
      // Intentionally left blank
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer<NullValue>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -6374308529927819177L;

      /**
       * @param output
       * @param object
       * @throws IOException
       * @see org.infinispan.marshall.Externalizer#writeObject(java.io.ObjectOutput,
       *      java.lang.Object)
       */
      @Override
      public void writeObject(ObjectOutput output, NullValue object) throws IOException {
         // Nothing to do?
      }

      /**
       * @param input
       * @return
       * @throws IOException
       * @throws ClassNotFoundException
       * @see org.infinispan.marshall.Externalizer#readObject(java.io.ObjectInput)
       */
      @Override
      public NullValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return NullValue.NULL;
      }
   }
}