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
package org.infinispan.cdi.util.defaultbean;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.infinispan.cdi.util.InjectableMethod;

// TODO Make this passivation capable
class DefaultProducerMethod<T, X> extends AbstractDefaultProducerBean<T> {

    private final InjectableMethod<X> producerMethod;
    private final InjectableMethod<X> disposerMethod;

    static <T, X> DefaultProducerMethod<T, X> of(Bean<T> originalBean, Type declaringBeanType, Set<Type> beanTypes, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, AnnotatedMethod<X> method, AnnotatedMethod<X> disposerMethod, BeanManager beanManager) {
        return new DefaultProducerMethod<T, X>(originalBean, declaringBeanType, beanTypes, qualifiers, declaringBeanQualifiers, method, disposerMethod, beanManager);
    }

    DefaultProducerMethod(Bean<T> originalBean, Type declaringBeanType, Set<Type> beanTypes, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, AnnotatedMethod<X> method, AnnotatedMethod<X> disposerMethod, BeanManager beanManager) {
        super(originalBean, declaringBeanType, beanTypes, qualifiers, declaringBeanQualifiers, beanManager);
        this.producerMethod = new InjectableMethod<X>(method, this, beanManager);
        if (disposerMethod != null) {
            this.disposerMethod = new InjectableMethod<X>(disposerMethod, this, beanManager);
        } else {
            this.disposerMethod = null;
        }
    }

    @Override
    protected T getValue(Object receiver, CreationalContext<T> creationalContext) {
        return producerMethod.invoke(receiver, creationalContext);
    }

    @Override
    public void destroy(T instance, CreationalContext<T> creationalContext) {
        if (disposerMethod != null) {
            try {
                disposerMethod.invoke(getReceiver(creationalContext), creationalContext);
            } finally {
                creationalContext.release();
            }
        }
    }

}
