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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

/**
 * A successful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class SuccessfulResponse extends ValidResponse {
   public static final SuccessfulResponse SUCCESSFUL_EMPTY_RESPONSE = new SuccessfulResponse(null);

   private final Object responseValue;

   private SuccessfulResponse(Object responseValue) {
      this.responseValue = responseValue;
   }

   public static SuccessfulResponse create(Object responseValue) {
      return responseValue == null ? SUCCESSFUL_EMPTY_RESPONSE : new SuccessfulResponse(responseValue);
   }    

   @Override
   public boolean isSuccessful() {
      return true;
   }

   public Object getResponseValue() {
      return responseValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SuccessfulResponse that = (SuccessfulResponse) o;

      if (responseValue != null ? !responseValue.equals(that.responseValue) : that.responseValue != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return responseValue != null ? responseValue.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "SuccessfulResponse{" +
            "responseValue=" + responseValue +
            "} ";
   }

   public static class Externalizer extends AbstractExternalizer<SuccessfulResponse> {
      @Override
      public void writeObject(ObjectOutput output, SuccessfulResponse response) throws IOException {
         if (response.responseValue == null) {
            output.writeBoolean(false);
         } else {
            output.writeBoolean(true);
            output.writeObject(response.responseValue);
         }
      }

      @Override
      public SuccessfulResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         boolean nonNullResponse = input.readBoolean();
         if (nonNullResponse) {
            return new SuccessfulResponse(input.readObject());
         } else {
            return SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
         }
      }

      @Override
      public Integer getId() {
         return Ids.SUCCESSFUL_RESPONSE;
      }

      @Override
      public Set<Class<? extends SuccessfulResponse>> getTypeClasses() {
         return Util.<Class<? extends SuccessfulResponse>>asSet(SuccessfulResponse.class);
      }
   }

}
