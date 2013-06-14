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

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.Map;

import javax.enterprise.inject.spi.AnnotatedConstructor;

/**
 * @author Stuart Douglas
 */
class AnnotatedConstructorImpl<X> extends AnnotatedCallableImpl<X, Constructor<X>> implements AnnotatedConstructor<X> {

   AnnotatedConstructorImpl(AnnotatedTypeImpl<X> type, Constructor<?> constructor, AnnotationStore annotations, Map<Integer, AnnotationStore> parameterAnnotations, Map<Integer, Type> typeOverrides) {
      super(type, (Constructor<X>) constructor, constructor.getDeclaringClass(), constructor.getParameterTypes(), getGenericArray(constructor), annotations, parameterAnnotations, null, typeOverrides);
   }

   private static Type[] getGenericArray(Constructor<?> constructor) {
      Type[] genericTypes = constructor.getGenericParameterTypes();
      // for inner classes genericTypes and parameterTypes can be different
      // length, this is a hack to fix this.
      // TODO: investigate this behavior further, on different JVM's and
      // compilers
      if (genericTypes.length + 1 == constructor.getParameterTypes().length) {
         genericTypes = new Type[constructor.getGenericParameterTypes().length + 1];
         genericTypes[0] = constructor.getParameterTypes()[0];
         for (int i = 0; i < constructor.getGenericParameterTypes().length; ++i) {
            genericTypes[i + 1] = constructor.getGenericParameterTypes()[i];
         }
      }
      return genericTypes;
   }

}
