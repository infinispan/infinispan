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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.enterprise.inject.spi.AnnotatedConstructor;
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedType;

/**
 * AnnotatedType implementation for adding beans in the BeforeBeanDiscovery
 * event
 *
 * @author Stuart Douglas
 */
class AnnotatedTypeImpl<X> extends AnnotatedImpl implements AnnotatedType<X> {

   private final Set<AnnotatedConstructor<X>> constructors;
   private final Set<AnnotatedField<? super X>> fields;
   private final Set<AnnotatedMethod<? super X>> methods;

   private final Class<X> javaClass;

   /**
    * We make sure that there is a NewAnnotatedMember for every public
    * method/field/constructor
    * <p/>
    * If annotation have been added to other methods as well we add them to
    */
   AnnotatedTypeImpl(Class<X> clazz, AnnotationStore typeAnnotations, Map<Field, AnnotationStore> fieldAnnotations, Map<Method, AnnotationStore> methodAnnotations, Map<Method, Map<Integer, AnnotationStore>> methodParameterAnnotations, Map<Constructor<?>, AnnotationStore> constructorAnnotations, Map<Constructor<?>, Map<Integer, AnnotationStore>> constructorParameterAnnotations, Map<Field, Type> fieldTypes, Map<Method, Map<Integer, Type>> methodParameterTypes, Map<Constructor<?>, Map<Integer, Type>> constructorParameterTypes) {
      super(clazz, typeAnnotations, null, null);
      this.javaClass = clazz;
      this.constructors = new HashSet<AnnotatedConstructor<X>>();
      Set<Constructor<?>> cset = new HashSet<Constructor<?>>();
      Set<Method> mset = new HashSet<Method>();
      Set<Field> fset = new HashSet<Field>();
      for (Constructor<?> c : clazz.getConstructors()) {
         AnnotatedConstructor<X> nc = new AnnotatedConstructorImpl<X>(this, c, constructorAnnotations.get(c), constructorParameterAnnotations.get(c), constructorParameterTypes.get(c));
         constructors.add(nc);
         cset.add(c);
      }
      for (Entry<Constructor<?>, AnnotationStore> c : constructorAnnotations.entrySet()) {
         if (!cset.contains(c.getKey())) {
            AnnotatedConstructor<X> nc = new AnnotatedConstructorImpl<X>(this, c.getKey(), c.getValue(), constructorParameterAnnotations.get(c.getKey()), constructorParameterTypes.get(c.getKey()));
            constructors.add(nc);
         }
      }
      this.methods = new HashSet<AnnotatedMethod<? super X>>();
      for (Method m : clazz.getMethods()) {
         if (!m.getDeclaringClass().equals(Object.class)) {
            AnnotatedMethodImpl<X> met = new AnnotatedMethodImpl<X>(this, m, methodAnnotations.get(m), methodParameterAnnotations.get(m), methodParameterTypes.get(m));
            methods.add(met);
            mset.add(m);
         }
      }
      for (Entry<Method, AnnotationStore> c : methodAnnotations.entrySet()) {
         if (!c.getKey().getDeclaringClass().equals(Object.class) && !mset.contains(c.getKey())) {
            AnnotatedMethodImpl<X> nc = new AnnotatedMethodImpl<X>(this, c.getKey(), c.getValue(), methodParameterAnnotations.get(c.getKey()), methodParameterTypes.get(c.getKey()));
            methods.add(nc);
         }
      }
      this.fields = new HashSet<AnnotatedField<? super X>>();
      for (Field f : clazz.getFields()) {
         AnnotatedField<X> b = new AnnotatedFieldImpl<X>(this, f, fieldAnnotations.get(f), fieldTypes.get(f));
         fields.add(b);
         fset.add(f);
      }
      for (Entry<Field, AnnotationStore> e : fieldAnnotations.entrySet()) {
         if (!fset.contains(e.getKey())) {
            fields.add(new AnnotatedFieldImpl<X>(this, e.getKey(), e.getValue(), fieldTypes.get(e.getKey())));
         }
      }
   }

   public Set<AnnotatedConstructor<X>> getConstructors() {
      return Collections.unmodifiableSet(constructors);
   }

   public Set<AnnotatedField<? super X>> getFields() {
      return Collections.unmodifiableSet(fields);
   }

   public Class<X> getJavaClass() {
      return javaClass;
   }

   public Set<AnnotatedMethod<? super X>> getMethods() {
      return Collections.unmodifiableSet(methods);
   }

}
