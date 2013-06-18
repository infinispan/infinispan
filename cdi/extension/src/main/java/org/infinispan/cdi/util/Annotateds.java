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
package org.infinispan.cdi.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.AnnotatedCallable;
import javax.enterprise.inject.spi.AnnotatedConstructor;
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedParameter;
import javax.enterprise.inject.spi.AnnotatedType;

/**
 * <p>
 * Utilities for working with {@link Annotated}s.
 * </p>
 * <p/>
 * <p>
 * Includes utilities to check the equality of and create unique id's for
 * <code>Annotated</code> instances.
 * </p>
 *
 * @author Stuart Douglas <stuart@baileyroberts.com.au>
 */
public class Annotateds {

    /**
     * Does the first stage of comparing AnnoatedCallables, however it cannot
     * compare the method parameters
     */
    private static class AnnotatedCallableComparator<T> implements Comparator<AnnotatedCallable<? super T>> {

        public int compare(AnnotatedCallable<? super T> arg0, AnnotatedCallable<? super T> arg1) {
            // compare the names first
            int result = (arg0.getJavaMember().getName().compareTo(arg1.getJavaMember().getName()));
            if (result != 0) {
                return result;
            }
            result = arg0.getJavaMember().getDeclaringClass().getName().compareTo(arg1.getJavaMember().getDeclaringClass().getName());
            if (result != 0) {
                return result;
            }
            result = arg0.getParameters().size() - arg1.getParameters().size();
            return result;
        }

    }

    private static class AnnotatedMethodComparator<T> implements Comparator<AnnotatedMethod<? super T>> {

        public static <T> Comparator<AnnotatedMethod<? super T>> instance() {
            return new AnnotatedMethodComparator<T>();
        }

        private AnnotatedCallableComparator<T> callableComparator = new AnnotatedCallableComparator<T>();

        public int compare(AnnotatedMethod<? super T> arg0, AnnotatedMethod<? super T> arg1) {
            int result = callableComparator.compare(arg0, arg1);
            if (result != 0) {
                return result;
            }
            for (int i = 0; i < arg0.getJavaMember().getParameterTypes().length; ++i) {
                Class<?> p0 = arg0.getJavaMember().getParameterTypes()[i];
                Class<?> p1 = arg1.getJavaMember().getParameterTypes()[i];
                result = p0.getName().compareTo(p1.getName());
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }

    }

    private static class AnnotatedConstructorComparator<T> implements Comparator<AnnotatedConstructor<? super T>> {

        public static <T> Comparator<AnnotatedConstructor<? super T>> instance() {
            return new AnnotatedConstructorComparator<T>();
        }

        private AnnotatedCallableComparator<T> callableComparator = new AnnotatedCallableComparator<T>();

        public int compare(AnnotatedConstructor<? super T> arg0, AnnotatedConstructor<? super T> arg1) {
            int result = callableComparator.compare(arg0, arg1);
            if (result != 0) {
                return result;
            }
            for (int i = 0; i < arg0.getJavaMember().getParameterTypes().length; ++i) {
                Class<?> p0 = arg0.getJavaMember().getParameterTypes()[i];
                Class<?> p1 = arg1.getJavaMember().getParameterTypes()[i];
                result = p0.getName().compareTo(p1.getName());
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }

    }

    private static class AnnotatedFieldComparator<T> implements Comparator<AnnotatedField<? super T>> {

        public static <T> Comparator<AnnotatedField<? super T>> instance() {
            return new AnnotatedFieldComparator<T>();
        }

        public int compare(AnnotatedField<? super T> arg0, AnnotatedField<? super T> arg1) {
            if (arg0.getJavaMember().getName().equals(arg1.getJavaMember().getName())) {
                return arg0.getJavaMember().getDeclaringClass().getName().compareTo(arg1.getJavaMember().getDeclaringClass().getName());
            }
            return arg0.getJavaMember().getName().compareTo(arg1.getJavaMember().getName());
        }

    }

    private static class AnnotationComparator implements Comparator<Annotation> {

        public static final Comparator<Annotation> INSTANCE = new AnnotationComparator();

        public int compare(Annotation arg0, Annotation arg1) {
            return arg0.annotationType().getName().compareTo(arg1.annotationType().getName());
        }
    }

    private static class MethodComparator implements Comparator<Method> {

        public static final Comparator<Method> INSTANCE = new MethodComparator();

        public int compare(Method arg0, Method arg1) {
            return arg0.getName().compareTo(arg1.getName());
        }
    }

    private static final char SEPERATOR = ';';

    private Annotateds() {
    }

    /**
     * Generates a deterministic signature for an {@link AnnotatedType}. Two
     * <code>AnnotatedType</code>s that have the same annotations and underlying
     * type will generate the same signature.
     * <p/>
     * This can be used to create a unique bean id for a passivation capable bean
     * that is added directly through the SPI.
     *
     * @param annotatedType The type to generate a signature for
     * @return A string representation of the annotated type
     */
    public static <X> String createTypeId(AnnotatedType<X> annotatedType) {
        return createTypeId(annotatedType.getJavaClass(), annotatedType.getAnnotations(), annotatedType.getMethods(), annotatedType.getFields(), annotatedType.getConstructors());
    }

    /**
     * Generates a unique signature for a concrete class. Annotations are not
     * read directly from the class, but are read from the
     * <code>annotations</code>, <code>methods</code>, <code>fields</code> and
     * <code>constructors</code> arguments
     *
     * @param clazz        The java class tyoe
     * @param annotations  Annotations present on the java class
     * @param methods      The AnnotatedMethods to include in the signature
     * @param fields       The AnnotatedFields to include in the signature
     * @param constructors The AnnotatedConstructors to include in the signature
     * @return A string representation of the type
     */
    public static <X> String createTypeId(Class<X> clazz, Collection<Annotation> annotations, Collection<AnnotatedMethod<? super X>> methods, Collection<AnnotatedField<? super X>> fields, Collection<AnnotatedConstructor<X>> constructors) {
        StringBuilder builder = new StringBuilder();

        builder.append(clazz.getName());
        builder.append(createAnnotationCollectionId(annotations));
        builder.append("{");

        // now deal with the fields
        List<AnnotatedField<? super X>> sortedFields = new ArrayList<AnnotatedField<? super X>>();
        sortedFields.addAll(fields);
        Collections.sort(sortedFields, AnnotatedFieldComparator.<X>instance());
        for (AnnotatedField<? super X> field : sortedFields) {
            if (!field.getAnnotations().isEmpty()) {
                builder.append(createFieldId(field));
                builder.append(SEPERATOR);
            }
        }

        // methods
        List<AnnotatedMethod<? super X>> sortedMethods = new ArrayList<AnnotatedMethod<? super X>>();
        sortedMethods.addAll(methods);
        Collections.sort(sortedMethods, AnnotatedMethodComparator.<X>instance());
        for (AnnotatedMethod<? super X> method : sortedMethods) {
            if (!method.getAnnotations().isEmpty() || hasMethodParameters(method)) {
                builder.append(createCallableId(method));
                builder.append(SEPERATOR);
            }
        }

        // constructors
        List<AnnotatedConstructor<? super X>> sortedConstructors = new ArrayList<AnnotatedConstructor<? super X>>();
        sortedConstructors.addAll(constructors);
        Collections.sort(sortedConstructors, AnnotatedConstructorComparator.<X>instance());
        for (AnnotatedConstructor<? super X> constructor : sortedConstructors) {
            if (!constructor.getAnnotations().isEmpty() || hasMethodParameters(constructor)) {
                builder.append(createCallableId(constructor));
                builder.append(SEPERATOR);
            }
        }
        builder.append("}");

        return builder.toString();
    }

    /**
     * Generates a deterministic signature for an {@link AnnotatedField}. Two
     * <code>AnnotatedField</code>s that have the same annotations and
     * underlying field will generate the same signature.
     */
    public static <X> String createFieldId(AnnotatedField<X> field) {
        return createFieldId(field.getJavaMember(), field.getAnnotations());
    }

    /**
     * Creates a deterministic signature for a {@link Field}.
     *
     * @param field       The field to generate the signature for
     * @param annotations The annotations to include in the signature
     */
    public static <X> String createFieldId(Field field, Collection<Annotation> annotations) {
        StringBuilder builder = new StringBuilder();
        builder.append(field.getDeclaringClass().getName());
        builder.append('.');
        builder.append(field.getName());
        builder.append(createAnnotationCollectionId(annotations));
        return builder.toString();
    }

    /**
     * Generates a deterministic signature for an {@link AnnotatedCallable}. Two
     * <code>AnnotatedCallable</code>s that have the same annotations and
     * underlying callable will generate the same signature.
     */
    public static <X> String createCallableId(AnnotatedCallable<X> method) {
        StringBuilder builder = new StringBuilder();
        builder.append(method.getJavaMember().getDeclaringClass().getName());
        builder.append('.');
        builder.append(method.getJavaMember().getName());
        builder.append(createAnnotationCollectionId(method.getAnnotations()));
        builder.append(createParameterListId(method.getParameters()));
        return builder.toString();
    }

    /**
     * Generates a unique string representation of a list of
     * {@link AnnotatedParameter}s.
     */
    public static <X> String createParameterListId(List<AnnotatedParameter<X>> parameters) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        for (int i = 0; i < parameters.size(); ++i) {
            AnnotatedParameter<X> ap = parameters.get(i);
            builder.append(createParameterId(ap));
            if (i + 1 != parameters.size()) {
                builder.append(',');
            }
        }
        builder.append(")");
        return builder.toString();
    }

    /**
     * Creates a string representation of an {@link AnnotatedParameter}.
     */
    public static <X> String createParameterId(AnnotatedParameter<X> annotatedParameter) {
        return createParameterId(annotatedParameter.getBaseType(), annotatedParameter.getAnnotations());
    }

    /**
     * Creates a string representation of a given type and set of annotations.
     */
    public static <X> String createParameterId(Type type, Set<Annotation> annotations) {
        StringBuilder builder = new StringBuilder();
        if (type instanceof Class<?>) {
            Class<?> c = (Class<?>) type;
            builder.append(c.getName());
        } else {
            builder.append(type.toString());
        }
        builder.append(createAnnotationCollectionId(annotations));
        return builder.toString();
    }

    private static <X> boolean hasMethodParameters(AnnotatedCallable<X> callable) {
        for (AnnotatedParameter<X> parameter : callable.getParameters()) {
            if (!parameter.getAnnotations().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static String createAnnotationCollectionId(Collection<Annotation> annotations) {
        if (annotations.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        builder.append('[');

        List<Annotation> annotationList = new ArrayList<Annotation>(annotations.size());
        annotationList.addAll(annotations);
        Collections.sort(annotationList, AnnotationComparator.INSTANCE);

        for (Annotation a : annotationList) {
            builder.append('@');
            builder.append(a.annotationType().getName());
            builder.append('(');
            Method[] declaredMethods = a.annotationType().getDeclaredMethods();
            List<Method> methods = new ArrayList<Method>(declaredMethods.length);
            for (Method m : declaredMethods) {
                methods.add(m);
            }
            Collections.sort(methods, MethodComparator.INSTANCE);

            for (int i = 0; i < methods.size(); ++i) {
                Method method = methods.get(i);
                try {
                    Object value = method.invoke(a);
                    builder.append(method.getName());
                    builder.append('=');
                    builder.append(value.toString());
                } catch (NullPointerException e) {
                    throw new RuntimeException("NullPointerException accessing annotation member, annotation:" + a.annotationType().getName() + " member: " + method.getName(), e);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException("IllegalArgumentException accessing annotation member, annotation:" + a.annotationType().getName() + " member: " + method.getName(), e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("IllegalAccessException accessing annotation member, annotation:" + a.annotationType().getName() + " member: " + method.getName(), e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("InvocationTargetException accessing annotation member, annotation:" + a.annotationType().getName() + " member: " + method.getName(), e);
                }
                if (i + 1 != methods.size()) {
                    builder.append(',');
                }
            }
            builder.append(')');
        }
        builder.append(']');
        return builder.toString();
    }
}
