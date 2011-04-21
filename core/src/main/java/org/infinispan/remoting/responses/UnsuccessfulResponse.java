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
package org.infinispan.remoting.responses;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * An unsuccessful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = UnsuccessfulResponse.Externalizer.class, id = Ids.UNSUCCESSFUL_RESPONSE)
public class UnsuccessfulResponse extends ValidResponse {
   public static final UnsuccessfulResponse INSTANCE = new UnsuccessfulResponse();

   private UnsuccessfulResponse() {
   }

   public boolean isSuccessful() {
      return false;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null) return false;
      return o.getClass().equals(this.getClass());
   }

   @Override
   public int hashCode() {
      return 13;
   }
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         // no-op
      }
      
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }
   }
}
