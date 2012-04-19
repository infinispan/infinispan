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

package org.infinispan.marshall.jboss;

import org.infinispan.marshall.Externalizer;
import org.jboss.marshalling.Creator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class JBossExternalizerAdapter implements org.jboss.marshalling.Externalizer {

   final Externalizer<Object> externalizer;

   public JBossExternalizerAdapter(Externalizer<?> externalizer) {
      this.externalizer = (Externalizer<Object>) externalizer;
   }

   @Override
   public void writeExternal(Object subject, ObjectOutput output) throws IOException {
      externalizer.writeObject(output, subject);
   }

   @Override
   public Object createExternal(Class<?> subjectType, ObjectInput input, Creator defaultCreator) throws IOException, ClassNotFoundException {
      return externalizer.readObject(input);
   }

   @Override
   public void readExternal(Object subject, ObjectInput input) throws IOException, ClassNotFoundException {
      // No-op
   }

}
