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
package org.infinispan.remoting.responses;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * An unsuccessful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnsuccessfulResponse extends ValidResponse {
   public static final UnsuccessfulResponse INSTANCE = new UnsuccessfulResponse();

   private UnsuccessfulResponse() {
   }

   @Override
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

   public static class Externalizer extends AbstractExternalizer<UnsuccessfulResponse> {
      @Override
      public void writeObject(ObjectOutput output, UnsuccessfulResponse object) throws IOException {
         // no-op
      }
      
      @Override
      public UnsuccessfulResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }

      @Override
      public Integer getId() {
         return Ids.UNSUCCESSFUL_RESPONSE;
      }

      @Override
      public Set<Class<? extends UnsuccessfulResponse>> getTypeClasses() {
         return Util.<Class<? extends UnsuccessfulResponse>>asSet(UnsuccessfulResponse.class);
      }
   }
}
