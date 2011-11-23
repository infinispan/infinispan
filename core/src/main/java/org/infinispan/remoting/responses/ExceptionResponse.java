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

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;

/**
 * A response that encapsulates an exception
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ExceptionResponse extends InvalidResponse {
   private Exception exception;

   public ExceptionResponse() {
   }

   public ExceptionResponse(Exception exception) {
      this.exception = exception;
   }

   public Exception getException() {
      return exception;
   }

   public void setException(Exception exception) {
      this.exception = exception;
   }

   public static class Externalizer extends AbstractExternalizer<ExceptionResponse> {
      @Override
      public void writeObject(ObjectOutput output, ExceptionResponse response) throws IOException {
         output.writeObject(response.exception);
      }

      @Override
      public ExceptionResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ExceptionResponse((Exception) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.EXCEPTION_RESPONSE;
      }

      @Override
      public Set<Class<? extends ExceptionResponse>> getTypeClasses() {
         return Util.<Class<? extends ExceptionResponse>>asSet(ExceptionResponse.class);
      }
   }
}
