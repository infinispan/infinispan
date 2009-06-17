/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.marshall.jboss.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.jcip.annotations.Immutable;

import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.jboss.Externalizer;

/**
 * TransientCacheValueExternalizer.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class TransientCacheValueExternalizer implements Externalizer {

   public void writeObject(ObjectOutput output, Object subject) throws IOException {
      TransientCacheValue icv = (TransientCacheValue) subject;
      output.writeObject(icv.getValue());
      UnsignedNumeric.writeUnsignedLong(output, icv.getLastUsed());
      output.writeLong(icv.getMaxIdle()); // could be negative so should not use unsigned longs
   }

   public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object v = input.readObject();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      Long maxIdle = input.readLong();
      return InternalEntryFactory.createValue(v, -1, -1, lastUsed, maxIdle);
   }

}
