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

import java.lang.reflect.Type;

import javax.enterprise.inject.spi.AnnotatedCallable;
import javax.enterprise.inject.spi.AnnotatedParameter;

/**
 * @author Stuart Douglas
 */
class AnnotatedParameterImpl<X> extends AnnotatedImpl implements AnnotatedParameter<X> {

   private final int position;
   private final AnnotatedCallable<X> declaringCallable;

   AnnotatedParameterImpl(AnnotatedCallable<X> declaringCallable, Class<?> type, int position, AnnotationStore annotations, Type genericType, Type typeOverride) {
      super(type, annotations, genericType, typeOverride);
      this.declaringCallable = declaringCallable;
      this.position = position;
   }

   public AnnotatedCallable<X> getDeclaringCallable() {
      return declaringCallable;
   }

   public int getPosition() {
      return position;
   }

}
