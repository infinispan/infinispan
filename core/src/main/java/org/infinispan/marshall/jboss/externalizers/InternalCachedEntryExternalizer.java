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

import net.jcip.annotations.Immutable;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.marshall.jboss.MarshallUtil;
import org.jboss.marshalling.Creator;
import org.jboss.marshalling.Externalizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * InternalCachedEntryExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 * @deprecated Use individual cached entry externalizers instead
 */
@Immutable
@Deprecated
public class InternalCachedEntryExternalizer implements Externalizer {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -3475239737916428837L;

   public void writeExternal(Object subject, ObjectOutput output) throws IOException {
      InternalCacheEntry ice = (InternalCacheEntry) subject;
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
      if (ice.canExpire()) {
         output.writeBoolean(true);
         MarshallUtil.writeUnsignedLong(output, ice.getCreated());
         output.writeLong(ice.getLifespan()); // could be negative so should not use unsigned longs
         MarshallUtil.writeUnsignedLong(output, ice.getLastUsed());
         output.writeLong(ice.getMaxIdle()); // could be negative so should not use unsigned longs
      } else {
         output.writeBoolean(false);
      }
   }

   public Object createExternal(Class<?> subjectType, ObjectInput input, Creator defaultCreator)
         throws IOException, ClassNotFoundException {
      Object k = input.readObject();
      Object v = input.readObject();
      boolean canExpire = input.readBoolean();
      if (canExpire) {
         long created = MarshallUtil.readUnsignedLong(input);
         long lifespan = input.readLong(); // could be negative so should not use unsigned longs
         long lastUsed = MarshallUtil.readUnsignedLong(input);
         long maxIdle = input.readLong(); // could be negative so should not use unsigned longs
         return InternalEntryFactory.create(k, v, created, lifespan, lastUsed, maxIdle);
      } else {
         return InternalEntryFactory.create(k, v);
      }
   }

   public void readExternal(Object subject, ObjectInput input) throws IOException,
                                                                      ClassNotFoundException {
      // No-op
   }
}
