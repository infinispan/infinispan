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
 * Indicates that the request was ignored,
 *
 * @author Jason T. Greene
 */
public class RequestIgnoredResponse extends InvalidResponse {
   public static final RequestIgnoredResponse INSTANCE = new RequestIgnoredResponse();

   private RequestIgnoredResponse() {
   }

   @Override
   public boolean isValid() {
      return true;
   }

   @Override
   public String toString() {
      return "RequestIgnoredResponse";
   }

   public static class Externalizer extends AbstractExternalizer<RequestIgnoredResponse> {
      @Override
      public void writeObject(ObjectOutput output, RequestIgnoredResponse object) throws IOException {
         // no-op
      }

      @Override
      public RequestIgnoredResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }

      @Override
      public Integer getId() {
         return Ids.REQUEST_IGNORED_RESPONSE;
      }

      @Override
      public Set<Class<? extends RequestIgnoredResponse>> getTypeClasses() {
         return Util.<Class<? extends RequestIgnoredResponse>>asSet(RequestIgnoredResponse.class);
      }
   }
}
