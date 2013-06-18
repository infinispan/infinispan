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

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;

/**
 * Utility class for working with JDK Reflection and also CDI's
 * {@link Annotated} metadata.
 *
 * @author Stuart Douglas
 * @author Pete Muir
 */
public class Reflections {

    /**
     * An empty array of type {@link Annotation}, useful converting lists to
     * arrays.
     */
    public static final Annotation[] EMPTY_ANNOTATION_ARRAY = new Annotation[0];

    /**
     * An empty array of type {@link Object}, useful for converting lists to
     * arrays.
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    public static final Type[] EMPTY_TYPES = {};

    public static final Class<?>[] EMPTY_CLASSES = new Class<?>[0];

    /**
     * <p>
     * Perform a runtime cast. Similar to {@link Class#cast(Object)}, but useful
     * when you do not have a {@link Class} object for type you wish to cast to.
     * </p>
     * <p/>
     * <p>
     * {@link Class#cast(Object)} should be used if possible
     * </p>
     *
     * @param <T> the type to cast to
     * @param obj the object to perform the cast on
     * @return the casted object
     * @throws ClassCastException if the type T is not a subtype of the object
     * @see Class#cast(Object)
     */
    @SuppressWarnings("unchecked")
    public static <T> T cast(Object obj) {
        return (T) obj;
    }

    /**
     * Get all the declared methods on the class hierarchy. This <b>will</b>
     * return overridden methods.
     *
     * @param clazz The class to search
     * @return the set of all declared methods or an empty set if there are none
     */
    public static Set<Method> getAllDeclaredMethods(Class<?> clazz) {
        HashSet<Method> methods = new HashSet<Method>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Method a : c.getDeclaredMethods()) {
                methods.add(a);
            }
        }
        return methods;
    }

    private static String buildInvokeMethodErrorMessage(Method method, Object obj, Object... args) {
        StringBuilder message = new StringBuilder(String.format("Exception invoking method [%s] on object [%s], using arguments [", method.getName(), obj));
        if (args != null)
            for (int i = 0; i < args.length; i++)
                message.append((i > 0 ? "," : "") + args[i]);
        message.append("]");
        return message.toString();
    }

    /**
     * Set the accessibility flag on the {@link AccessibleObject} as described in
     * {@link AccessibleObject#setAccessible(boolean)} within the context of
     * a {@link PrivilegedAction}.
     *
     * @param <A>    member the accessible object type
     * @param member the accessible object
     * @return the accessible object after the accessible flag has been altered
     */
    public static <A extends AccessibleObject> A setAccessible(A member) {
        AccessController.doPrivileged(new SetAccessiblePriviligedAction(member));
        return member;
    }

    /**
     * <p>
     * Invoke the specified method on the provided instance, passing any additional
     * arguments included in this method as arguments to the specified method.
     * </p>
     * <p/>
     * <p>
     * This method attempts to set the accessible flag of the method in a
     * {@link PrivilegedAction} before invoking the method if the first argument
     * is true.
     * </p>
     * <p/>
     * <p>This method provides the same functionality and throws the same exceptions as
     * {@link Reflections#invokeMethod(boolean, Method, Class, Object, Object...)}, with the
     * expected return type set to {@link Object}.</p>
     *
     * @see Reflections#invokeMethod(boolean, Method, Class, Object, Object...)
     * @see Method#invoke(Object, Object...)
     */
    public static Object invokeMethod(boolean setAccessible, Method method, Object instance, Object... args) {
        return invokeMethod(setAccessible, method, Object.class, instance, args);
    }

    /**
     * <p>
     * Invoke the specified method on the provided instance, passing any additional
     * arguments included in this method as arguments to the specified method.
     * </p>
     * <p/>
     * <p>This method provides the same functionality and throws the same exceptions as
     * {@link Reflections#invokeMethod(boolean, Method, Class, Object, Object...)}, with the
     * expected return type set to {@link Object} and honoring the accessibility of
     * the method.</p>
     *
     * @see Reflections#invokeMethod(boolean, Method, Class, Object, Object...)
     * @see Method#invoke(Object, Object...)
     */
    public static <T> T invokeMethod(Method method, Class<T> expectedReturnType, Object instance, Object... args) {
        return invokeMethod(false, method, expectedReturnType, instance, args);
    }

    /**
     * <p>
     * Invoke the method on the instance, with any arguments specified, casting
     * the result of invoking the method to the expected return type.
     * </p>
     * <p/>
     * <p>
     * This method wraps {@link Method#invoke(Object, Object...)}, converting the
     * checked exceptions that {@link Method#invoke(Object, Object...)} specifies
     * to runtime exceptions.
     * </p>
     * <p/>
     * <p>
     * If instructed, this method attempts to set the accessible flag of the method in a
     * {@link PrivilegedAction} before invoking the method.
     * </p>
     *
     * @param setAccessible flag indicating whether method should first be set as
     *                      accessible
     * @param method        the method to invoke
     * @param instance      the instance to invoke the method
     * @param args          the arguments to the method
     * @return the result of invoking the method, or null if the method's return
     *         type is void
     * @throws RuntimeException            if this <code>Method</code> object enforces Java
     *                                     language access control and the underlying method is
     *                                     inaccessible or if the underlying method throws an exception or
     *                                     if the initialization provoked by this method fails.
     * @throws IllegalArgumentException    if the method is an instance method and
     *                                     the specified <code>instance</code> argument is not an instance
     *                                     of the class or interface declaring the underlying method (or
     *                                     of a subclass or implementor thereof); if the number of actual
     *                                     and formal parameters differ; if an unwrapping conversion for
     *                                     primitive arguments fails; or if, after possible unwrapping, a
     *                                     parameter value cannot be converted to the corresponding formal
     *                                     parameter type by a method invocation conversion.
     * @throws NullPointerException        if the specified <code>instance</code> is
     *                                     null and the method is an instance method.
     * @throws ClassCastException          if the result of invoking the method cannot be
     *                                     cast to the expectedReturnType
     * @throws ExceptionInInitializerError if the initialization provoked by this
     *                                     method fails.
     * @see Method#invoke(Object, Object...)
     */
    public static <T> T invokeMethod(boolean setAccessible, Method method, Class<T> expectedReturnType, Object instance, Object... args) {
        if (setAccessible && !method.isAccessible()) {
            setAccessible(method);
        }

        try {
            return expectedReturnType.cast(method.invoke(instance, args));
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(buildInvokeMethodErrorMessage(method, instance, args), ex);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(buildInvokeMethodErrorMessage(method, instance, args), ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(buildInvokeMethodErrorMessage(method, instance, args), ex.getCause());
        } catch (NullPointerException ex) {
            NullPointerException ex2 = new NullPointerException(buildInvokeMethodErrorMessage(method, instance, args));
            ex2.initCause(ex.getCause());
            throw ex2;
        } catch (ExceptionInInitializerError e) {
            ExceptionInInitializerError e2 = new ExceptionInInitializerError(buildInvokeMethodErrorMessage(method, instance, args));
            e2.initCause(e.getCause());
            throw e2;
        }
    }

    private static String buildGetFieldValueErrorMessage(Field field, Object obj) {
        return String.format("Exception reading [%s] field from object [%s].", field.getName(), obj);
    }

    /**
     * <p>
     * Get the value of the field, on the specified instance, casting the value
     * of the field to the expected type.
     * </p>
     * <p/>
     * <p>
     * This method wraps {@link Field#get(Object)}, converting the checked
     * exceptions that {@link Field#get(Object)} specifies to runtime exceptions.
     * </p>
     *
     * @param <T>          the type of the field's value
     * @param field        the field to operate on
     * @param instance     the instance from which to retrieve the value
     * @param expectedType the expected type of the field's value
     * @return the value of the field
     * @throws RuntimeException            if the underlying field is inaccessible.
     * @throws IllegalArgumentException    if the specified <code>instance</code> is not an
     *                                     instance of the class or interface declaring the underlying
     *                                     field (or a subclass or implementor thereof).
     * @throws NullPointerException        if the specified <code>instance</code> is null and the field
     *                                     is an instance field.
     * @throws ExceptionInInitializerError if the initialization provoked by this
     *                                     method fails.
     */
	public static <T> T getFieldValue(Field field, Object instance, Class<T> expectedType) {
        try {
            return Reflections.cast(field.get(instance));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(buildGetFieldValueErrorMessage(field, instance), e);
        } catch (NullPointerException ex) {
            NullPointerException ex2 = new NullPointerException(buildGetFieldValueErrorMessage(field, instance));
            ex2.initCause(ex.getCause());
            throw ex2;
        } catch (ExceptionInInitializerError e) {
            ExceptionInInitializerError e2 = new ExceptionInInitializerError(buildGetFieldValueErrorMessage(field, instance));
            e2.initCause(e.getCause());
            throw e2;
        }
    }

    /**
     * Extract the raw type, given a type.
     *
     * @param <T>  the type
     * @param type the type to extract the raw type from
     * @return the raw type, or null if the raw type cannot be determined.
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getRawType(Type type) {
        if (type instanceof Class<?>) {
            return (Class<T>) type;
        } else if (type instanceof ParameterizedType) {
            if (((ParameterizedType) type).getRawType() instanceof Class<?>) {
                return (Class<T>) ((ParameterizedType) type).getRawType();
            }
        }
        return null;
    }

    /**
     * Check if a class is serializable.
     *
     * @param clazz The class to check
     * @return true if the class implements serializable or is a primitive
     */
    public static boolean isSerializable(Class<?> clazz) {
        return clazz.isPrimitive() || Serializable.class.isAssignableFrom(clazz);
    }
    
    /**
     * Check the assignability of one type to another, taking into account the
     * actual type arguements
     *
     * @param rawType1             the raw type of the class to check
     * @param actualTypeArguments1 the actual type arguements to check, or an
     *                             empty array if not a parameterized type
     * @param rawType2             the raw type of the class to check
     * @param actualTypeArguments2 the actual type arguements to check, or an
     *                             empty array if not a parameterized type
     * @return
     */
    public static boolean isAssignableFrom(Class<?> rawType1, Type[] actualTypeArguments1, Class<?> rawType2, Type[] actualTypeArguments2) {
        return Types.boxedClass(rawType1).isAssignableFrom(Types.boxedClass(rawType2)) && isAssignableFrom(actualTypeArguments1, actualTypeArguments2);
    }

    public static boolean matches(Class<?> rawType1, Type[] actualTypeArguments1, Class<?> rawType2, Type[] actualTypeArguments2) {
        return Types.boxedClass(rawType1).equals(Types.boxedClass(rawType2)) && isAssignableFrom(actualTypeArguments1, actualTypeArguments2);
    }

    public static boolean isAssignableFrom(Type[] actualTypeArguments1, Type[] actualTypeArguments2) {
        for (int i = 0; i < actualTypeArguments1.length; i++) {
            Type type1 = actualTypeArguments1[i];
            Type type2 = Object.class;
            if (actualTypeArguments2.length > i) {
                type2 = actualTypeArguments2[i];
            }
            if (!isAssignableFrom(type1, type2)) {
                return false;
            }
        }
        return true;
    }

    public static boolean matches(Type type1, Set<? extends Type> types2) {
        for (Type type2 : types2) {
            if (matches(type1, type2)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAssignableFrom(Type type1, Type[] types2) {
        for (Type type2 : types2) {
            if (isAssignableFrom(type1, type2)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAssignableFrom(Type type1, Type type2) {
        if (type1 instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type1;
            if (isAssignableFrom(clazz, EMPTY_TYPES, type2)) {
                return true;
            }
        }
        if (type1 instanceof ParameterizedType) {
            ParameterizedType parameterizedType1 = (ParameterizedType) type1;
            if (parameterizedType1.getRawType() instanceof Class<?>) {
                if (isAssignableFrom((Class<?>) parameterizedType1.getRawType(), parameterizedType1.getActualTypeArguments(), type2)) {
                    return true;
                }
            }
        }
        if (type1 instanceof WildcardType) {
            WildcardType wildcardType = (WildcardType) type1;
            if (isTypeBounded(type2, wildcardType.getLowerBounds(), wildcardType.getUpperBounds())) {
                return true;
            }
        }
        if (type2 instanceof WildcardType) {
            WildcardType wildcardType = (WildcardType) type2;
            if (isTypeBounded(type1, wildcardType.getUpperBounds(), wildcardType.getLowerBounds())) {
                return true;
            }
        }
        if (type1 instanceof TypeVariable<?>) {
            TypeVariable<?> typeVariable = (TypeVariable<?>) type1;
            if (isTypeBounded(type2, EMPTY_TYPES, typeVariable.getBounds())) {
                return true;
            }
        }
        if (type2 instanceof TypeVariable<?>) {
            TypeVariable<?> typeVariable = (TypeVariable<?>) type2;
            if (isTypeBounded(type1, typeVariable.getBounds(), EMPTY_TYPES)) {
                return true;
            }
        }
        return false;
    }

    public static boolean matches(Type type1, Type type2) {
        if (type1 instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type1;
            if (matches(clazz, EMPTY_TYPES, type2)) {
                return true;
            }
        }
        if (type1 instanceof ParameterizedType) {
            ParameterizedType parameterizedType1 = (ParameterizedType) type1;
            if (parameterizedType1.getRawType() instanceof Class<?>) {
                if (matches((Class<?>) parameterizedType1.getRawType(), parameterizedType1.getActualTypeArguments(), type2)) {
                    return true;
                }
            }
        }
        if (type1 instanceof WildcardType) {
            WildcardType wildcardType = (WildcardType) type1;
            if (isTypeBounded(type2, wildcardType.getLowerBounds(), wildcardType.getUpperBounds())) {
                return true;
            }
        }
        if (type2 instanceof WildcardType) {
            WildcardType wildcardType = (WildcardType) type2;
            if (isTypeBounded(type1, wildcardType.getUpperBounds(), wildcardType.getLowerBounds())) {
                return true;
            }
        }
        if (type1 instanceof TypeVariable<?>) {
            TypeVariable<?> typeVariable = (TypeVariable<?>) type1;
            if (isTypeBounded(type2, EMPTY_TYPES, typeVariable.getBounds())) {
                return true;
            }
        }
        if (type2 instanceof TypeVariable<?>) {
            TypeVariable<?> typeVariable = (TypeVariable<?>) type2;
            if (isTypeBounded(type1, typeVariable.getBounds(), EMPTY_TYPES)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isTypeBounded(Type type, Type[] lowerBounds, Type[] upperBounds) {
        if (lowerBounds.length > 0) {
            if (!isAssignableFrom(type, lowerBounds)) {
                return false;
            }
        }
        if (upperBounds.length > 0) {
            if (!isAssignableFrom(upperBounds, type)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAssignableFrom(Class<?> rawType1, Type[] actualTypeArguments1, Type type2) {
        if (type2 instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type2;
            if (parameterizedType.getRawType() instanceof Class<?>) {
                if (isAssignableFrom(rawType1, actualTypeArguments1, (Class<?>) parameterizedType.getRawType(), parameterizedType.getActualTypeArguments())) {
                    return true;
                }
            }
        } else if (type2 instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type2;
            if (isAssignableFrom(rawType1, actualTypeArguments1, clazz, EMPTY_TYPES)) {
                return true;
            }
        } else if (type2 instanceof TypeVariable<?>) {
            TypeVariable<?> typeVariable = (TypeVariable<?>) type2;
            if (isTypeBounded(rawType1, actualTypeArguments1, typeVariable.getBounds())) {
                return true;
            }
        }
        return false;
    }

    public static boolean matches(Class<?> rawType1, Type[] actualTypeArguments1, Type type2) {
        if (type2 instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type2;
            if (parameterizedType.getRawType() instanceof Class<?>) {
                if (matches(rawType1, actualTypeArguments1, (Class<?>) parameterizedType.getRawType(), parameterizedType.getActualTypeArguments())) {
                    return true;
                }
            }
        } else if (type2 instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type2;
            if (matches(rawType1, actualTypeArguments1, clazz, EMPTY_TYPES)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether whether any of the types1 matches a type in types2
     *
     * @param types1
     * @param types2
     * @return
     */
    public static boolean matches(Set<Type> types1, Set<Type> types2) {
        for (Type type : types1) {
            if (matches(type, types2)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAssignableFrom(Type[] types1, Type type2) {
        for (Type type : types1) {
            if (isAssignableFrom(type, type2)) {
                return true;
            }
        }
        return false;
    }


    private Reflections() {
    }

    /**
     * Inspects an annotated element for the given meta annotation. This should
     * only be used for user defined meta annotations, where the annotation must
     * be physically present.
     *
     * @param element        The element to inspect
     * @param annotationType The meta annotation to search for
     * @return The annotation instance found on this element or null if no
     *         matching annotation was found.
     */
    public static <A extends Annotation> A getMetaAnnotation(Annotated element, final Class<A> annotationType) {
        for (Annotation annotation : element.getAnnotations()) {
            if (annotation.annotationType().isAnnotationPresent(annotationType)) {
                return annotation.annotationType().getAnnotation(annotationType);
            }
        }
        return null;
    }

    /**
     * Extract the qualifiers from a set of annotations.
     *
     * @param beanManager the beanManager to use to determine if an annotation is
     *                    a qualifier
     * @param annotations the annotations to check
     * @return any qualifiers present in <code>annotations</code>
     */
    @SuppressWarnings("unchecked")
    public static Set<Annotation> getQualifiers(BeanManager beanManager, Iterable<Annotation> annotations) {
        return getQualifiers(beanManager, new Iterable[]{annotations});
    }

    /**
     * Extract the qualifiers from a set of annotations.
     *
     * @param beanManager the beanManager to use to determine if an annotation is
     *                    a qualifier
     * @param annotations the annotations to check
     * @return any qualifiers present in <code>annotations</code>
     */
    public static Set<Annotation> getQualifiers(BeanManager beanManager, Iterable<Annotation>... annotations) {
        Set<Annotation> qualifiers = new HashSet<Annotation>();
        for (Iterable<Annotation> annotationSet : annotations) {
            for (Annotation annotation : annotationSet) {
                if (beanManager.isQualifier(annotation.annotationType())) {
                    qualifiers.add(annotation);
                }
            }
        }
        return qualifiers;
    }
    
    /**
     * Get all the declared fields on the class hierarchy. This <b>will</b>
     * return overridden fields.
     *
     * @param clazz The class to search
     * @return the set of all declared fields or an empty set if there are none
     */
    public static Set<Field> getAllDeclaredFields(Class<?> clazz) {
        HashSet<Field> fields = new HashSet<Field>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field a : c.getDeclaredFields()) {
                fields.add(a);
            }
        }
        return fields;
    }

}
