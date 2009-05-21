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

import org.infinispan.CacheException;
import org.infinispan.marshall.jboss.ClassExternalizer;
import org.infinispan.marshall.jboss.MarshallUtil;
import org.infinispan.marshall.jboss.Externalizer;
import org.infinispan.util.Util;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;
import java.util.Map;

/**
 * Map externalizer for all map implementations except immutable maps and singleton maps, i.e. FastCopyHashMap, HashMap,
 * TreeMap.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class MapExternalizer implements Externalizer, ClassExternalizer.ClassWritable {
   /** The serialVersionUID */
   private static final long serialVersionUID = -532896252671303391L;
   private ClassExternalizer classRw;

   public void writeObject(Marshaller output, Object subject) throws IOException {
      classRw.writeClass(output, subject.getClass());
      MarshallUtil.marshallMap((Map) subject, output);
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      Class<?> subjectType = classRw.readClass(input);
      Map subject = null;
      try {
         subject = (Map) Util.getInstance(subjectType);
      } catch (Exception e) {
         throw new CacheException("Unable to create new instance of ReplicableCommand", e);
      }
      MarshallUtil.unmarshallMap(subject, input);
      return subject;
   }

   public void setClassExternalizer(ClassExternalizer classRw) {
      this.classRw = classRw;
   }
}
