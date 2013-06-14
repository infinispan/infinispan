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

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.AnnotatedConstructor;
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedParameter;
import javax.enterprise.inject.spi.AnnotatedType;

/**
 * <p> Class for constructing a new AnnotatedType. A new instance of builder
 * should be used for each annotated type. </p> <p/> <p> {@link
 * AnnotatedTypeBuilder} is not thread-safe. </p>
 *
 * @author Stuart Douglas
 * @author Pete Muir
 * @see AnnotatedType
 */
public class AnnotatedTypeBuilder<X> {

   private static final Log log = LogFactory.getLog(AnnotatedTypeBuilder.class, Log.class);

   private Class<X> javaClass;
   private final AnnotationBuilder typeAnnotations;

   private final Map<Constructor<?>, AnnotationBuilder> constructors;
   private final Map<Constructor<?>, Map<Integer, AnnotationBuilder>> constructorParameters;
   private final Map<Constructor<?>, Map<Integer, Type>> constructorParameterTypes;

   private final Map<Field, AnnotationBuilder> fields;
   private final Map<Field, Type> fieldTypes;

   private final Map<Method, AnnotationBuilder> methods;
   private final Map<Method, Map<Integer, AnnotationBuilder>> methodParameters;
   private final Map<Method, Map<Integer, Type>> methodParameterTypes;

   /**
    * Create a new builder. A new builder has no annotations and no members.
    *
    * @see #readFromType(AnnotatedType)
    * @see #readFromType(Class)
    * @see #readFromType(AnnotatedType, boolean)
    * @see #readFromType(Class, boolean)
    */
   public AnnotatedTypeBuilder() {
      this.typeAnnotations = new AnnotationBuilder();
      this.constructors = new HashMap<Constructor<?>, AnnotationBuilder>();
      this.constructorParameters = new HashMap<Constructor<?>, Map<Integer, AnnotationBuilder>>();
      this.constructorParameterTypes = new HashMap<Constructor<?>, Map<Integer, Type>>();
      this.fields = new HashMap<Field, AnnotationBuilder>();
      this.fieldTypes = new HashMap<Field, Type>();
      this.methods = new HashMap<Method, AnnotationBuilder>();
      this.methodParameters = new HashMap<Method, Map<Integer, AnnotationBuilder>>();
      this.methodParameterTypes = new HashMap<Method, Map<Integer, Type>>();
   }

   /**
    * Add an annotation to the type declaration.
    *
    * @param annotation the annotation instance to add
    * @throws IllegalArgumentException if the annotation is null
    */
   public AnnotatedTypeBuilder<X> addToClass(Annotation annotation) {
      typeAnnotations.add(annotation);
      return this;
   }

   /**
    * Reads in from an existing AnnotatedType. Any elements not present are
    * added. The javaClass will be read in. If the annotation already exists on
    * that element in the builder the read annotation will be used.
    *
    * @param type the type to read from
    * @throws IllegalArgumentException if type is null
    */
   public AnnotatedTypeBuilder<X> readFromType(AnnotatedType<X> type) {
      return readFromType(type, true);
   }

   /**
    * Reads in from an existing AnnotatedType. Any elements not present are
    * added. The javaClass will be read in if overwrite is true. If the
    * annotation already exists on that element in the builder, overwrite
    * determines whether the original or read annotation will be used.
    *
    * @param type      the type to read from
    * @param overwrite if true, the read annotation will replace any existing
    *                  annotation
    * @throws IllegalArgumentException if type is null
    */
   public AnnotatedTypeBuilder<X> readFromType(AnnotatedType<X> type, boolean overwrite) {
      if (type == null) {
         throw log.parameterMustNotBeNull("type");
      }
      if (javaClass == null || overwrite) {
         this.javaClass = type.getJavaClass();
      }
      mergeAnnotationsOnElement(type, overwrite, typeAnnotations);
      for (AnnotatedField<? super X> field : type.getFields()) {
         if (fields.get(field.getJavaMember()) == null) {
            fields.put(field.getJavaMember(), new AnnotationBuilder());
         }
         mergeAnnotationsOnElement(field, overwrite, fields.get(field.getJavaMember()));
      }
      for (AnnotatedMethod<? super X> method : type.getMethods()) {
         if (methods.get(method.getJavaMember()) == null) {
            methods.put(method.getJavaMember(), new AnnotationBuilder());
         }
         mergeAnnotationsOnElement(method, overwrite, methods.get(method.getJavaMember()));
         for (AnnotatedParameter<? super X> p : method.getParameters()) {
            if (methodParameters.get(method.getJavaMember()) == null) {
               methodParameters.put(method.getJavaMember(), new HashMap<Integer, AnnotationBuilder>());
            }
            if (methodParameters.get(method.getJavaMember()).get(p.getPosition()) == null) {
               methodParameters.get(method.getJavaMember()).put(p.getPosition(), new AnnotationBuilder());
            }
            mergeAnnotationsOnElement(p, overwrite, methodParameters.get(method.getJavaMember()).get(p.getPosition()));
         }
      }
      for (AnnotatedConstructor<? super X> constructor : type.getConstructors()) {
         if (constructors.get(constructor.getJavaMember()) == null) {
            constructors.put(constructor.getJavaMember(), new AnnotationBuilder());
         }
         mergeAnnotationsOnElement(constructor, overwrite, constructors.get(constructor.getJavaMember()));
         for (AnnotatedParameter<? super X> p : constructor.getParameters()) {
            if (constructorParameters.get(constructor.getJavaMember()) == null) {
               constructorParameters.put(constructor.getJavaMember(), new HashMap<Integer, AnnotationBuilder>());
            }
            if (constructorParameters.get(constructor.getJavaMember()).get(p.getPosition()) == null) {
               constructorParameters.get(constructor.getJavaMember()).put(p.getPosition(), new AnnotationBuilder());
            }
            mergeAnnotationsOnElement(p, overwrite, constructorParameters.get(constructor.getJavaMember()).get(p.getPosition()));
         }
      }
      return this;
   }

   /**
    * Reads the annotations from an existing java type. Annotations already
    * present will be overwritten
    *
    * @param type the type to read from
    * @throws IllegalArgumentException if type is null
    */
   public AnnotatedTypeBuilder<X> readFromType(Class<X> type) {
      return readFromType(type, true);
   }

   /**
    * Reads the annotations from an existing java type. If overwrite is true
    * then existing annotations will be overwritten
    *
    * @param type      the type to read from
    * @param overwrite if true, the read annotation will replace any existing
    *                  annotation
    */
   public AnnotatedTypeBuilder<X> readFromType(Class<X> type, boolean overwrite) {
      if (type == null) {
         throw log.parameterMustNotBeNull("type");
      }
      if (javaClass == null || overwrite) {
         this.javaClass = type;
      }
      for (Annotation annotation : type.getAnnotations()) {
         if (overwrite || !typeAnnotations.isAnnotationPresent(annotation.annotationType())) {
            typeAnnotations.add(annotation);
         }
      }

      for (Field field : Reflections.getAllDeclaredFields(type)) {
         AnnotationBuilder annotationBuilder = fields.get(field);
         if (annotationBuilder == null) {
            annotationBuilder = new AnnotationBuilder();
            fields.put(field, annotationBuilder);
         }
         field.setAccessible(true);
         for (Annotation annotation : field.getAnnotations()) {
            if (overwrite || !annotationBuilder.isAnnotationPresent(annotation.annotationType())) {
               annotationBuilder.add(annotation);
            }
         }
      }

      for (Method method : Reflections.getAllDeclaredMethods(type)) {
         AnnotationBuilder annotationBuilder = methods.get(method);
         if (annotationBuilder == null) {
            annotationBuilder = new AnnotationBuilder();
            methods.put(method, annotationBuilder);
         }
         method.setAccessible(true);
         for (Annotation annotation : method.getAnnotations()) {
            if (overwrite || !annotationBuilder.isAnnotationPresent(annotation.annotationType())) {
               annotationBuilder.add(annotation);
            }
         }

         Map<Integer, AnnotationBuilder> parameters = methodParameters.get(method);
         if (parameters == null) {
            parameters = new HashMap<Integer, AnnotationBuilder>();
            methodParameters.put(method, parameters);
         }
         for (int i = 0; i < method.getParameterTypes().length; ++i) {
            AnnotationBuilder parameterAnnotationBuilder = parameters.get(i);
            if (parameterAnnotationBuilder == null) {
               parameterAnnotationBuilder = new AnnotationBuilder();
               parameters.put(i, parameterAnnotationBuilder);
            }
            for (Annotation annotation : method.getParameterAnnotations()[i]) {
               if (overwrite || !parameterAnnotationBuilder.isAnnotationPresent(annotation.annotationType())) {
                  parameterAnnotationBuilder.add(annotation);
               }
            }
         }
      }

      for (Constructor<?> constructor : type.getDeclaredConstructors()) {
         AnnotationBuilder annotationBuilder = constructors.get(constructor);
         if (annotationBuilder == null) {
            annotationBuilder = new AnnotationBuilder();
            constructors.put(constructor, annotationBuilder);
         }
         constructor.setAccessible(true);
         for (Annotation annotation : constructor.getAnnotations()) {
            if (overwrite || !annotationBuilder.isAnnotationPresent(annotation.annotationType())) {
               annotationBuilder.add(annotation);
            }
         }
         Map<Integer, AnnotationBuilder> mparams = constructorParameters.get(constructor);
         if (mparams == null) {
            mparams = new HashMap<Integer, AnnotationBuilder>();
            constructorParameters.put(constructor, mparams);
         }
         for (int i = 0; i < constructor.getParameterTypes().length; ++i) {
            AnnotationBuilder parameterAnnotationBuilder = mparams.get(i);
            if (parameterAnnotationBuilder == null) {
               parameterAnnotationBuilder = new AnnotationBuilder();
               mparams.put(i, parameterAnnotationBuilder);
            }
            for (Annotation annotation : constructor.getParameterAnnotations()[i]) {
               if (overwrite || !parameterAnnotationBuilder.isAnnotationPresent(annotation.annotationType())) {
                  annotationBuilder.add(annotation);
               }
            }
         }
      }
      return this;
   }

   protected void mergeAnnotationsOnElement(Annotated annotated, boolean overwriteExisting, AnnotationBuilder typeAnnotations) {
      for (Annotation annotation : annotated.getAnnotations()) {
         if (typeAnnotations.getAnnotation(annotation.annotationType()) != null) {
            if (overwriteExisting) {
               typeAnnotations.remove(annotation.annotationType());
               typeAnnotations.add(annotation);
            }
         } else {
            typeAnnotations.add(annotation);
         }
      }
   }

   /**
    * Create an {@link AnnotatedType}. Any public members present on the
    * underlying class and not overridden by the builder will be automatically
    * added.
    */
   public AnnotatedType<X> create() {
      Map<Constructor<?>, Map<Integer, AnnotationStore>> constructorParameterAnnnotations = new HashMap<Constructor<?>, Map<Integer, AnnotationStore>>();
      Map<Constructor<?>, AnnotationStore> constructorAnnotations = new HashMap<Constructor<?>, AnnotationStore>();
      Map<Method, Map<Integer, AnnotationStore>> methodParameterAnnnotations = new HashMap<Method, Map<Integer, AnnotationStore>>();
      Map<Method, AnnotationStore> methodAnnotations = new HashMap<Method, AnnotationStore>();
      Map<Field, AnnotationStore> fieldAnnotations = new HashMap<Field, AnnotationStore>();

      for (Entry<Field, AnnotationBuilder> field : fields.entrySet()) {
         fieldAnnotations.put(field.getKey(), field.getValue().create());
      }

      for (Entry<Method, AnnotationBuilder> method : methods.entrySet()) {
         methodAnnotations.put(method.getKey(), method.getValue().create());
      }
      for (Entry<Method, Map<Integer, AnnotationBuilder>> parameters : methodParameters.entrySet()) {
         Map<Integer, AnnotationStore> parameterAnnotations = new HashMap<Integer, AnnotationStore>();
         methodParameterAnnnotations.put(parameters.getKey(), parameterAnnotations);
         for (Entry<Integer, AnnotationBuilder> parameter : parameters.getValue().entrySet()) {
            parameterAnnotations.put(parameter.getKey(), parameter.getValue().create());
         }
      }

      for (Entry<Constructor<?>, AnnotationBuilder> constructor : constructors.entrySet()) {
         constructorAnnotations.put(constructor.getKey(), constructor.getValue().create());
      }
      for (Entry<Constructor<?>, Map<Integer, AnnotationBuilder>> parameters : constructorParameters.entrySet()) {
         Map<Integer, AnnotationStore> parameterAnnotations = new HashMap<Integer, AnnotationStore>();
         constructorParameterAnnnotations.put(parameters.getKey(), parameterAnnotations);
         for (Entry<Integer, AnnotationBuilder> parameter : parameters.getValue().entrySet()) {
            parameterAnnotations.put(parameter.getKey(), parameter.getValue().create());
         }
      }

      return new AnnotatedTypeImpl<X>(javaClass, typeAnnotations.create(), fieldAnnotations, methodAnnotations, methodParameterAnnnotations, constructorAnnotations, constructorParameterAnnnotations, fieldTypes, methodParameterTypes, constructorParameterTypes);
   }

}
