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

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.InjectionPoint;

/**
 * An implementation of {@link Bean} that forwards all calls to the
 * {@link #delegate()}.
 *
 * @param <T> the class of the bean
 * @author Pete Muir
 */
public abstract class ForwardingBean<T> implements Bean<T> {

    /**
     * All calls to this {@link Bean} instance are forwarded to the delegate
     * unless overridden.
     *
     * @return the delegate {@link Bean}
     */
    protected abstract Bean<T> delegate();

    public Class<?> getBeanClass() {
        return delegate().getBeanClass();
    }

    public Set<InjectionPoint> getInjectionPoints() {
        return delegate().getInjectionPoints();
    }

    public String getName() {
        return delegate().getName();
    }

    public Set<Annotation> getQualifiers() {
        return delegate().getQualifiers();
    }

    public Class<? extends Annotation> getScope() {
        return delegate().getScope();
    }

    public Set<Class<? extends Annotation>> getStereotypes() {
        return delegate().getStereotypes();
    }

    public Set<Type> getTypes() {
        return delegate().getTypes();
    }

    public boolean isAlternative() {
        return delegate().isAlternative();
    }

    public boolean isNullable() {
        return delegate().isNullable();
    }

    public T create(CreationalContext<T> creationalContext) {
        return delegate().create(creationalContext);
    }

    public void destroy(T instance, CreationalContext<T> creationalContext) {
        delegate().destroy(instance, creationalContext);
    }

    @Override
    public int hashCode() {
        return delegate().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return delegate().equals(obj);
    }

    @Override
    public String toString() {
        return delegate().toString();
    }

}
