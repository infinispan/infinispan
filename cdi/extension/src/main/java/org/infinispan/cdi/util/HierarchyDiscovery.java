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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for resolving all bean types from a given type.
 */
public class HierarchyDiscovery {

    private final Type type;

    private Map<Type, Class<?>> types;

    public HierarchyDiscovery(Type type) {
        this.type = type;
    }

    protected void add(Class<?> clazz, Type type) {
        types.put(type, clazz);
    }

    public Set<Type> getTypeClosure() {
        if (types == null) {
            init();
        }
        // Return an independent set with no ties to the BiMap used
        return new HashSet<Type>(types.keySet());
    }

    private void init() {
        this.types = new HashMap<Type, Class<?>>();
        try {
            discoverTypes(type);
        } catch (StackOverflowError e) {
            System.out.println("type" + type);
            Thread.dumpStack();
            throw e;
        }
    }

    public Type getResolvedType() {
        if (type instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type;
            return resolveType(clazz);
        }
        return type;
    }

    private void discoverTypes(Type type) {
        if (type != null) {
            if (type instanceof Class<?>) {
                Class<?> clazz = (Class<?>) type;
                add(clazz, resolveType(clazz));
                discoverFromClass(clazz);
            } else {
                Class<?> clazz = null;
                if (type instanceof ParameterizedType) {
                    Type rawType = ((ParameterizedType) type).getRawType();
                    if (rawType instanceof Class<?>) {
                        discoverFromClass((Class<?>) rawType);
                        clazz = (Class<?>) rawType;
                    }
                }
                add(clazz, type);
            }
        }
    }

    private Type resolveType(Class<?> clazz) {
        if (clazz.getTypeParameters().length > 0) {
            TypeVariable<?>[] actualTypeParameters = clazz.getTypeParameters();
            ParameterizedType parameterizedType = new ParameterizedTypeImpl(clazz, actualTypeParameters, clazz.getDeclaringClass());
            return parameterizedType;
        } else {
            return clazz;
        }
    }

    private void discoverFromClass(Class<?> clazz) {
        discoverTypes(resolveType(type, type, clazz.getGenericSuperclass()));
        for (Type c : clazz.getGenericInterfaces()) {
            discoverTypes(resolveType(type, type, c));
        }
    }

    /**
     * Gets the actual types by resolving TypeParameters.
     *
     * @param beanType
     * @param type
     * @return actual type
     */
    private Type resolveType(Type beanType, Type beanType2, Type type) {
        if (type instanceof ParameterizedType) {
            if (beanType instanceof ParameterizedType) {
                return resolveParameterizedType((ParameterizedType) beanType, (ParameterizedType) type);
            }
            if (beanType instanceof Class<?>) {
                return resolveType(((Class<?>) beanType).getGenericSuperclass(), beanType2, type);
            }
        }

        if (type instanceof TypeVariable<?>) {
            if (beanType instanceof ParameterizedType) {
                return resolveTypeParameter((ParameterizedType) beanType, beanType2, (TypeVariable<?>) type);
            }
            if (beanType instanceof Class<?>) {
                return resolveType(((Class<?>) beanType).getGenericSuperclass(), beanType2, type);
            }
        }
        return type;
    }

    private Type resolveParameterizedType(ParameterizedType beanType, ParameterizedType parameterizedType) {
        Type rawType = parameterizedType.getRawType();
        Type[] actualTypes = parameterizedType.getActualTypeArguments();

        Type resolvedRawType = resolveType(beanType, beanType, rawType);
        Type[] resolvedActualTypes = new Type[actualTypes.length];

        for (int i = 0; i < actualTypes.length; i++) {
            resolvedActualTypes[i] = resolveType(beanType, beanType, actualTypes[i]);
        }
        // reconstruct ParameterizedType by types resolved TypeVariable.
        return new ParameterizedTypeImpl(resolvedRawType, resolvedActualTypes, parameterizedType.getOwnerType());
    }

    private Type resolveTypeParameter(ParameterizedType type, Type beanType, TypeVariable<?> typeVariable) {
        // step1. raw type
        Class<?> actualType = (Class<?>) type.getRawType();
        TypeVariable<?>[] typeVariables = actualType.getTypeParameters();
        Type[] actualTypes = type.getActualTypeArguments();
        for (int i = 0; i < typeVariables.length; i++) {
            if (typeVariables[i].equals(typeVariable) && !actualTypes[i].equals(typeVariable)) {
                return resolveType(this.type, beanType, actualTypes[i]);
            }
        }

        // step2. generic super class
        Type genericSuperType = actualType.getGenericSuperclass();
        Type resolvedGenericSuperType = resolveType(genericSuperType, beanType, typeVariable);
        if (!(resolvedGenericSuperType instanceof TypeVariable<?>)) {
            return resolvedGenericSuperType;
        }

        // step3. generic interfaces
        if (beanType instanceof ParameterizedType) {
            for (Type interfaceType : ((Class<?>) ((ParameterizedType) beanType).getRawType()).getGenericInterfaces()) {
                Type resolvedType = resolveType(interfaceType, interfaceType, typeVariable);
                if (!(resolvedType instanceof TypeVariable<?>)) {
                    return resolvedType;
                }
            }
        }

        // don't resolve type variable
        return typeVariable;
    }

}
