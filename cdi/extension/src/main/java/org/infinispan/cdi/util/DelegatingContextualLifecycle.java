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

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.InjectionTarget;

/**
 * An implementation of {@link ContextualLifecycle} that is backed by an
 * {@link InjectionTarget}.
 *
 * @param <T>
 * @author Pete Muir
 * @author Stuart Douglas
 */
public class DelegatingContextualLifecycle<T> implements ContextualLifecycle<T> {

    private final InjectionTarget<T> injectionTarget;

    /**
     * Instantiate a new {@link ContextualLifecycle} backed by an
     * {@link InjectionTarget}.
     *
     * @param injectionTarget the {@link InjectionTarget} used to create and
     *                        destroy instances
     */
    public DelegatingContextualLifecycle(InjectionTarget<T> injectionTarget) {
        this.injectionTarget = injectionTarget;
    }

    public T create(Bean<T> bean, CreationalContext<T> creationalContext) {
        T instance = injectionTarget.produce(creationalContext);
        injectionTarget.inject(instance, creationalContext);
        injectionTarget.postConstruct(instance);
        return instance;
    }

    public void destroy(Bean<T> bean, T instance, CreationalContext<T> creationalContext) {
        try {
            injectionTarget.preDestroy(instance);
            creationalContext.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
