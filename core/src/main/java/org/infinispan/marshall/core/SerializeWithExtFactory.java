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

package org.infinispan.marshall.core;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.jboss.JBossExternalizerAdapter;
import org.infinispan.marshall.LegacyExternalizerAdapter;
import org.jboss.marshalling.AnnotationClassExternalizerFactory;
import org.jboss.marshalling.ClassExternalizerFactory;
import org.jboss.marshalling.Externalizer;

/**
 * JBoss Marshalling plugin class for {@link ClassExternalizerFactory} that allows for Infinispan
 * annotations to be used instead of JBoss Marshalling ones in order to discover which classes are
 * serializable with Infinispan externalizers.
 *
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 * @since 6.0
 */
public class SerializeWithExtFactory implements ClassExternalizerFactory {

   final ClassExternalizerFactory jbmarExtFactory = new AnnotationClassExternalizerFactory();

   @Override
   public Externalizer getExternalizer(Class<?> type) {
      try {
         final org.infinispan.commons.marshall.Externalizer<?> ext;
         SerializeWith ann = type.getAnnotation(SerializeWith.class);
         if (ann != null) {
            ext = ann.value().newInstance();
         } else {
            org.infinispan.marshall.SerializeWith legacy = type.getAnnotation(org.infinispan.marshall.SerializeWith.class);
            if (legacy != null) {
               ext = new LegacyExternalizerAdapter(legacy.value().newInstance());
            } else {
               // Check for JBoss Marshaller's @Externalize
               return jbmarExtFactory.getExternalizer(type);
            }
         }
         return new JBossExternalizerAdapter(ext);
      } catch (Exception e) {
         throw new IllegalArgumentException(String.format("Cannot instantiate externalizer for %s", type), e);
      }

   }
}
