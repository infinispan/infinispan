/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.compat;

import org.infinispan.CacheException;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.util.ByteArrayKey;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class ToStringConverter implements TypeConverter<Object, String> {

   // Use same marshaller as Hot Rod client
   // TODO: Make it configurable?
   private final StreamingMarshaller marshaller = new GenericJBossMarshaller();

   @Override
   public String convert(Object source) {
      if (source instanceof String)
         return (String) source;

      if (source instanceof byte[])
         return unmarshallString((byte[]) source);

      if (source instanceof ByteArrayKey)
         return unmarshallString(((ByteArrayKey) source).getData());

      return source.toString();
   }

   private String unmarshallString(byte[] source) {
      try {
         return marshaller.objectFromByteBuffer(source).toString();
      } catch (Exception e) {
         throw new CacheException("Unable to convert to String", e);
      }
   }

}
