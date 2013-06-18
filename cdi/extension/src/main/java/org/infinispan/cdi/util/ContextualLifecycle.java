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

/**
 * Callbacks used by {@link BeanBuilder} and {@link ImmutableBean} to allow control
 * of the creation and destruction of a custom bean.
 *
 * @param <T> the class of the bean instance
 * @author Stuart Douglas
 */
public interface ContextualLifecycle<T> {
    /**
     * Callback invoked by a created bean when
     * {@link Bean#create(CreationalContext)} is called.
     *
     * @param bean              the bean initiating the callback
     * @param creationalContext the context in which this instance was created
     */
    public T create(Bean<T> bean, CreationalContext<T> creationalContext);

    /**
     * Callback invoked by a created bean when
     * {@link Bean#destroy(Object, CreationalContext)} is called.
     *
     * @param bean              the bean initiating the callback
     * @param instance          the contextual instance to destroy
     * @param creationalContext the context in which this instance was created
     */
    public void destroy(Bean<T> bean, T instance, CreationalContext<T> creationalContext);

}
