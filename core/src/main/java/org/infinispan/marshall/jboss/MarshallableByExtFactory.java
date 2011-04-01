/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2011, Red Hat Middleware LLC, and individual contributors
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

package org.infinispan.marshall.jboss;

import org.infinispan.marshall.MarshallableBy;
import org.jboss.marshalling.AnnotationClassExternalizerFactory;
import org.jboss.marshalling.ClassExternalizerFactory;
import org.jboss.marshalling.Externalizer;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class MarshallableByExtFactory implements ClassExternalizerFactory {

   final ClassExternalizerFactory jbmarExtFactory = new AnnotationClassExternalizerFactory();

   @Override
   public Externalizer getExternalizer(Class<?> type) {
      MarshallableBy ann = type.getAnnotation(MarshallableBy.class);
      if (ann == null) {
         // Check for JBoss Marshaller's @Externalize
         return jbmarExtFactory.getExternalizer(type);
      } else {
         try {
            return new JBossExternalizerAdapter(ann.value().newInstance());
         } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                  "Cannot instantiate externalizer for %s", type), e);
         }
      }
   }
}
