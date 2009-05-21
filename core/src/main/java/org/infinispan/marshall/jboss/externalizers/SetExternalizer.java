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
import org.infinispan.CacheException;
import org.infinispan.marshall.jboss.ClassExternalizer;
import org.infinispan.marshall.jboss.MarshallUtil;
import org.infinispan.marshall.jboss.Externalizer;
import org.infinispan.util.Util;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Set externalizer for all set implementations, i.e. HashSet and TreeSet
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class SetExternalizer implements Externalizer, ClassExternalizer.ClassWritable {
   /** The serialVersionUID */
   private static final long serialVersionUID = -3147427397000304867L;
   private ClassExternalizer classExt;

   public void writeObject(Marshaller output, Object subject) throws IOException {
      classExt.writeClass(output, subject.getClass());
      MarshallUtil.marshallCollection((Collection) subject, output);
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      Class<?> subjectType = classExt.readClass(input);
      Set subject = null;
      try {
         subject = (Set) Util.getInstance(subjectType);
      } catch (Exception e) {
         throw new CacheException("Unable to create new instance of ReplicableCommand", e);
      }
      int size = MarshallUtil.readUnsignedInt(input);
      for (int i = 0; i < size; i++) subject.add(input.readObject());
      return subject;
   }

   public void setClassExternalizer(ClassExternalizer classExt) {
      this.classExt = classExt;
   }

}