/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.infinispan.jcache.annotation.solder;

import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;

import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.AnnotatedType;

/**
 * @author Stuart Douglas
 */
abstract class AnnotatedMemberImpl<X, M extends Member> extends AnnotatedImpl implements AnnotatedMember<X> {

   private final AnnotatedType<X> declaringType;
   private final M javaMember;

   protected AnnotatedMemberImpl(AnnotatedType<X> declaringType, M member, Class<?> memberType, AnnotationStore annotations, Type genericType, Type overridenType) {
      super(memberType, annotations, genericType, overridenType);
      this.declaringType = declaringType;
      this.javaMember = member;
   }

   public AnnotatedType<X> getDeclaringType() {
      return declaringType;
   }

   public M getJavaMember() {
      return javaMember;
   }

   public boolean isStatic() {
      return Modifier.isStatic(javaMember.getModifiers());
   }

}
