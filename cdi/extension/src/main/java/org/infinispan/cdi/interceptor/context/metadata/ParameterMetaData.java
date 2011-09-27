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
package org.infinispan.cdi.interceptor.context.metadata;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Contains the metadata for a parameter of a method annotated with A JCACHE annotation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class ParameterMetaData {

   private final Type baseType;
   private final Class<?> rawType;
   private final int position;
   private final Set<Annotation> annotations;

   public ParameterMetaData(Class<?> type, int position, Set<Annotation> annotations) {
      this.baseType = type.getGenericSuperclass();
      this.rawType = type;
      this.position = position;
      this.annotations = unmodifiableSet(annotations);
   }

   public Type getBaseType() {
      return baseType;
   }

   public Class<?> getRawType() {
      return rawType;
   }

   public int getPosition() {
      return position;
   }

   public Set<Annotation> getAnnotations() {
      return annotations;
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("ParameterMetaData{")
            .append("baseType=").append(baseType)
            .append(", rawType=").append(rawType)
            .append(", position=").append(position)
            .append(", annotations=").append(annotations)
            .append('}')
            .toString();
   }
}
