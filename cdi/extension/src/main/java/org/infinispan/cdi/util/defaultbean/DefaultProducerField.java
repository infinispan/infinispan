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
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.infinispan.cdi.util.Reflections;

// TODO Make this passivation capable
class DefaultProducerField<T, X> extends AbstractDefaultProducerBean<T> {

    private final AnnotatedField<X> field;

    static <T, X> DefaultProducerField<T, X> of(Bean<T> originalBean, Type declaringBeanType, Set<Type> beanTypes, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, AnnotatedField<X> field, BeanManager beanManager) {
        return new DefaultProducerField<T, X>(originalBean, declaringBeanType, beanTypes, qualifiers, declaringBeanQualifiers, field, beanManager);
    }

    DefaultProducerField(Bean<T> originalBean, Type declaringBeanType, Set<Type> beanTypes, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, AnnotatedField<X> field, BeanManager beanManager) {
        super(originalBean, declaringBeanType, beanTypes, qualifiers, declaringBeanQualifiers, beanManager);
        this.field = field;
        if (!field.getJavaMember().isAccessible()) {
            field.getJavaMember().setAccessible(true);
        }
    }

    @Override
    protected T getValue(Object receiver, CreationalContext<T> creationalContext) {
        return Reflections.getFieldValue(field.getJavaMember(), receiver, Reflections.<T>getRawType(field.getBaseType()));
    }

    @Override
    public void destroy(T instance, CreationalContext<T> creationalContext) {
        // TODO: disposers
        creationalContext.release();
    }

}
