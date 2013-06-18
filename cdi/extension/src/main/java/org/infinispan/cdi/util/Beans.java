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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedParameter;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;

/**
 * A set of utility methods for working with beans.
 *
 * @author Pete Muir
 */
public class Beans {

    private Beans() {
    }
    
    /**
     * Returns a new set with @Default and @Any added as needed
     * @return
     */
    public static Set<Annotation> buildQualifiers(Set<Annotation> annotations) {
        Set<Annotation> qualifiers = new HashSet<Annotation>(annotations);
        if (annotations.isEmpty()) {
            qualifiers.add(DefaultLiteral.INSTANCE);
        }
        qualifiers.add(AnyLiteral.INSTANCE);
        return qualifiers;
    }

    public static void checkReturnValue(Object instance, Bean<?> bean, InjectionPoint injectionPoint, BeanManager beanManager) {
        if (instance == null && !Dependent.class.equals(bean.getScope())) {
            throw new IllegalStateException("Cannot return null from a non-dependent producer method: " + bean);
        } else if (instance != null) {
            boolean passivating = beanManager.isPassivatingScope(bean.getScope());
            boolean instanceSerializable = Reflections.isSerializable(instance.getClass());
            if (passivating && !instanceSerializable) {
                throw new IllegalStateException("Producers cannot declare passivating scope and return a non-serializable class: " + bean);
            }
            if (injectionPoint != null && injectionPoint.getBean() != null) {
                if (!instanceSerializable && beanManager.isPassivatingScope(injectionPoint.getBean().getScope())) {
                    if (injectionPoint.getMember() instanceof Field) {
                        if (!injectionPoint.isTransient() && instance != null && !instanceSerializable) {
                            throw new IllegalStateException("Producers cannot produce non-serializable instances for injection into non-transient fields of passivating beans. Producer " + bean + "at injection point " + injectionPoint);
                        }
                    } else if (injectionPoint.getMember() instanceof Method) {
                        Method method = (Method) injectionPoint.getMember();
                        if (method.isAnnotationPresent(Inject.class)) {
                            throw new IllegalStateException("Producers cannot produce non-serializable instances for injection into parameters of initializers of beans declaring passivating scope. Producer " + bean + "at injection point " + injectionPoint);
                        }
                        if (method.isAnnotationPresent(Produces.class)) {
                            throw new IllegalStateException("Producers cannot produce non-serializable instances for injection into parameters of producer methods declaring passivating scope. Producer " + bean + "at injection point " + injectionPoint);
                        }
                    } else if (injectionPoint.getMember() instanceof Constructor<?>) {
                        throw new IllegalStateException("Producers cannot produce non-serializable instances for injection into parameters of constructors of beans declaring passivating scope. Producer " + bean + "at injection point " + injectionPoint);
                    }
                }
            }
        }
    }
    
    /**
     * Given a method, and the bean on which the method is declared, create a
     * collection of injection points representing the parameters of the method.
     *
     * @param <X>           the type declaring the method
     * @param method        the method
     * @param declaringBean the bean on which the method is declared
     * @param beanManager   the bean manager to use to create the injection points
     * @return the injection points
     */
    public static <X> List<InjectionPoint> createInjectionPoints(AnnotatedMethod<X> method, Bean<?> declaringBean, BeanManager beanManager) {
        List<InjectionPoint> injectionPoints = new ArrayList<InjectionPoint>();
        for (AnnotatedParameter<X> parameter : method.getParameters()) {
            InjectionPoint injectionPoint = new ImmutableInjectionPoint(parameter, beanManager, declaringBean, false, false);
            injectionPoints.add(injectionPoint);
        }
        return injectionPoints;
    }

}
