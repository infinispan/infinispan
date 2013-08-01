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

package org.infinispan.remoting.responses;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A response that signals the named cache is not running on the target node.
 *
 * @author Dan Berindei
 * @since 6.0
 */
public class CacheNotFoundResponse extends InvalidResponse {
   public static CacheNotFoundResponse INSTANCE = new CacheNotFoundResponse();

   public CacheNotFoundResponse() {
   }

   public static class Externalizer extends AbstractExternalizer<CacheNotFoundResponse> {
      @Override
      public void writeObject(ObjectOutput output, CacheNotFoundResponse response) throws IOException {
      }

      @Override
      public CacheNotFoundResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CacheNotFoundResponse();
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_NOT_FOUND_RESPONSE;
      }

      @Override
      public Set<Class<? extends CacheNotFoundResponse>> getTypeClasses() {
         return Util.<Class<? extends CacheNotFoundResponse>>asSet(CacheNotFoundResponse.class);
      }
   }
}
