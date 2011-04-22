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

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.Set;

/**
 * An unsure response - used with Dist - essentially asks the caller to check the next response from the next node since
 * the sender is in a state of flux (probably in the middle of rebalancing)
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnsureResponse extends ValidResponse {
   public static final UnsureResponse INSTANCE = new UnsureResponse();
   public boolean isSuccessful() {
      return false;
   }

   public static class Externalizer extends AbstractExternalizer<UnsureResponse> {
      @Override
      public void writeObject(ObjectOutput output, UnsureResponse subject) throws IOException {
      }

      @Override
      public UnsureResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }

      @Override
      public Integer getId() {
         return Ids.UNSURE_RESPONSE;
      }

      @Override
      public Set<Class<? extends UnsureResponse>> getTypeClasses() {
         return Util.<Class<? extends UnsureResponse>>asSet(UnsureResponse.class);
      }
   }
}
