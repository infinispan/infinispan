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
import java.lang.reflect.Type;
import java.util.Set;

import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.PassivationCapable;

/**
 * <p>
 * A base class for implementing a {@link PassivationCapable} {@link Bean}. The
 * attributes are immutable, and collections are defensively copied on
 * instantiation. It uses the defaults from the specification for properties if
 * not specified.
 * </p>
 * <p/>
 * <p>
 * This bean delegates it's lifecycle to the callbacks on the provided
 * {@link ContextualLifecycle}.
 * </p>
 *
 * @author Stuart Douglas
 * @author Pete Muir
 * @see ImmutableBean
 * @see BeanBuilder
 */
public class ImmutablePassivationCapableBean<T> extends ImmutableBean<T> implements PassivationCapable {
    private final String id;

    public ImmutablePassivationCapableBean(String id, Class<?> beanClass, String name, Set<Annotation> qualifiers, Class<? extends Annotation> scope, Set<Class<? extends Annotation>> stereotypes, Set<Type> types, boolean alternative, boolean nullable, Set<InjectionPoint> injectionPoints, ContextualLifecycle<T> beanLifecycle, String toString) {
        super(beanClass, name, qualifiers, scope, stereotypes, types, alternative, nullable, injectionPoints, beanLifecycle, toString);
        this.id = id;
    }

    public String getId() {
        return id;
    }

}
