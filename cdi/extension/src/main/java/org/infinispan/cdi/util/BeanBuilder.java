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

import static org.infinispan.cdi.util.Arrays2.asSet;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.PassivationCapable;
import javax.inject.Named;

/**
 * <p>
 * A builder class for creating immutable beans. The builder can create
 * {@link PassivationCapable} beans, using
 * {@link Annotateds#createTypeId(AnnotatedType)} to generate the id.
 * </p>
 * <p/>
 * <p>
 * The builder can read from an {@link AnnotatedType} and have any attribute
 * modified. This class is not thread-safe, but the bean created by calling
 * {@link #create()} is.
 * </p>
 * <p/>
 * <p>
 * It is advised that a new bean builder is instantiated for each bean created.
 * </p>
 *
 * @author Stuart Douglas
 * @author Pete Muir
 * @see ImmutableBean
 * @see ImmutablePassivationCapableBean
 */
public class BeanBuilder<T> {

    private final BeanManager beanManager;

    private Class<?> beanClass;
    private String name;
    private Set<Annotation> qualifiers;
    private Class<? extends Annotation> scope;
    private Set<Class<? extends Annotation>> stereotypes;
    private Set<Type> types;
    private Set<InjectionPoint> injectionPoints;
    private boolean alternative;
    private boolean nullable;
    private ContextualLifecycle<T> beanLifecycle;
    boolean passivationCapable;
    private String id;
    private String toString;

    /**
     * Instantiate a new bean builder.
     *
     * @param beanManager the bean manager to use for creating injection targets
     *                    and determining if annotations are qualifiers, scopes or
     *                    stereotypes.
     */
    public BeanBuilder(BeanManager beanManager) {
        this.beanManager = beanManager;
    }

    /**
     * <p>
     * Read the {@link AnnotatedType}, creating a bean from the class and it's
     * annotations.
     * </p>
     * <p/>
     * <p>
     * By default the bean lifecycle will wrap the result of calling
     * {@link BeanManager#createInjectionTarget(AnnotatedType)}.
     * </p>
     * <p/>
     * <p>
     * {@link BeanBuilder} does <em>not</em> support reading members of the class
     * to create producers or observer methods.
     * </p>
     *
     * @param type the type to read
     */
    public BeanBuilder<T> readFromType(AnnotatedType<T> type) {
        this.beanClass = type.getJavaClass();
        InjectionTarget<T> injectionTarget;
        if (!type.getJavaClass().isInterface()) {
            injectionTarget = beanManager.createInjectionTarget(type);
        } else {
            injectionTarget = new DummyInjectionTarget<T>();
        }
        this.beanLifecycle = new DelegatingContextualLifecycle<T>(injectionTarget);
        this.injectionPoints = injectionTarget.getInjectionPoints();
        this.qualifiers = new HashSet<Annotation>();
        this.stereotypes = new HashSet<Class<? extends Annotation>>();
        this.types = new HashSet<Type>();
        for (Annotation annotation : type.getAnnotations()) {
            if (beanManager.isQualifier(annotation.annotationType())) {
                this.qualifiers.add(annotation);
            } else if (beanManager.isScope(annotation.annotationType())) {
                this.scope = annotation.annotationType();
            } else if (beanManager.isStereotype(annotation.annotationType())) {
                this.stereotypes.add(annotation.annotationType());
            }
            if (annotation instanceof Named) {
                this.name = ((Named) annotation).value();
            }
            if (annotation instanceof Alternative) {
                this.alternative = true;
            }
        }
        if (this.scope == null) {
            this.scope = Dependent.class;
        }
        for (Class<?> c = type.getJavaClass(); c != Object.class && c != null; c = c.getSuperclass()) {
            this.types.add(c);
        }
        for (Class<?> i : type.getJavaClass().getInterfaces()) {
            this.types.add(i);
        }
        if (qualifiers.isEmpty()) {
            qualifiers.add(DefaultLiteral.INSTANCE);
        }
        qualifiers.add(AnyLiteral.INSTANCE);
        this.id = ImmutableBean.class.getName() + ":" + Annotateds.createTypeId(type);
        return this;
    }

    /**
     * <p>
     * Use the bean builder's current state to define the bean.
     * </p>
     *
     * @return the bean
     */
    public Bean<T> create() {
        if (!passivationCapable) {
            return new ImmutableBean<T>(beanClass, name, qualifiers, scope, stereotypes, types, alternative, nullable, injectionPoints, beanLifecycle, toString);
        } else {
            return new ImmutablePassivationCapableBean<T>(id, beanClass, name, qualifiers, scope, stereotypes, types, alternative, nullable, injectionPoints, beanLifecycle, toString);
        }
    }

    /**
     * Qualifiers currently defined for bean creation.
     *
     * @return the qualifiers current defined
     */
    public Set<Annotation> getQualifiers() {
        return qualifiers;
    }

    /**
     * Define the qualifiers used for bean creation.
     *
     * @param qualifiers the qualifiers to use
     */
    public BeanBuilder<T> qualifiers(Set<Annotation> qualifiers) {
        this.qualifiers = qualifiers;
        return this;
    }

    /**
     * Define the qualifiers used for bean creation.
     *
     * @param qualifiers the qualifiers to use
     */
    public BeanBuilder<T> qualifiers(Annotation... qualifiers) {
        this.qualifiers = asSet(qualifiers);
        return this;
    }

    /**
     * Add to the qualifiers used for bean creation.
     *
     * @param qualifiers the additional qualifier to use
     */
    public BeanBuilder<T> addQualifier(Annotation qualifier) {
        this.qualifiers.add(qualifier);
        return this;
    }

    /**
     * Add to the qualifiers used for bean creation.
     *
     * @param qualifiers the additional qualifiers to use
     */
    public BeanBuilder<T> addQualifiers(Annotation... qualifiers) {
        this.qualifiers.addAll(asSet(qualifiers));
        return this;
    }

    /**
     * Add to the qualifiers used for bean creation.
     *
     * @param qualifiers the additional qualifiers to use
     */
    public BeanBuilder<T> addQualifiers(Collection<Annotation> qualifiers) {
        this.qualifiers.addAll(qualifiers);
        return this;
    }

    /**
     * Scope currently defined for bean creation.
     *
     * @return the scope currently defined
     */
    public Class<? extends Annotation> getScope() {
        return scope;
    }

    /**
     * Define the scope used for bean creation.
     *
     * @param scope the scope to use
     */
    public BeanBuilder<T> scope(Class<? extends Annotation> scope) {
        this.scope = scope;
        return this;
    }

    /**
     * Stereotypes currently defined for bean creation.
     *
     * @return the stereotypes currently defined
     */
    public Set<Class<? extends Annotation>> getStereotypes() {
        return stereotypes;
    }

    /**
     * Define the stereotypes used for bean creation.
     *
     * @param stereotypes the stereotypes to use
     */
    public BeanBuilder<T> stereotypes(Set<Class<? extends Annotation>> stereotypes) {
        this.stereotypes = stereotypes;
        return this;
    }

    /**
     * Type closure currently defined for bean creation.
     *
     * @return the type closure currently defined
     */
    public Set<Type> getTypes() {
        return types;
    }

    /**
     * Define the type closure used for bean creation.
     *
     * @param types the type closure to use
     */
    public BeanBuilder<T> types(Set<Type> types) {
        this.types = types;
        return this;
    }

    /**
     * Define the type closure used for bean creation.
     *
     * @param types the type closure to use
     */
    public BeanBuilder<T> types(Type... types) {
        this.types = asSet(types);
        return this;
    }

    /**
     * Add to the type closure used for bean creation.
     *
     * @param type additional type to use
     */
    public BeanBuilder<T> addType(Type type) {
        this.types.add(type);
        return this;
    }

    /**
     * Add to the type closure used for bean creation.
     *
     * @param types the additional types to use
     */
    public BeanBuilder<T> addTypes(Type... types) {
        this.types.addAll(asSet(types));
        return this;
    }

    /**
     * Add to the type closure used for bean creation.
     *
     * @param types the additional types to use
     */
    public BeanBuilder<T> addTypes(Collection<Type> types) {
        this.types.addAll(types);
        return this;
    }

    /**
     * Whether the created bean will be an alternative.
     *
     * @return <code>true</code> if the created bean will be an alternative,
     *         otherwise <code>false</code>
     */
    public boolean isAlternative() {
        return alternative;
    }

    /**
     * Define that the created bean will (or will not) be an alternative.
     *
     * @param alternative <code>true</code> if the created bean should be an
     *                    alternative, otherwise <code>false</code>
     */
    public BeanBuilder<T> alternative(boolean alternative) {
        this.alternative = alternative;
        return this;
    }

    /**
     * Whether the created bean will be nullable.
     *
     * @return <code>true</code> if the created bean will be nullable, otherwise
     *         <code>false</code>
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Define that the created bean will (or will not) be nullable.
     *
     * @param nullable <code>true</code> if the created bean should be nullable,
     *                 otherwise <code>false</code>
     */
    public BeanBuilder<T> nullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    /**
     * The {@link ContextualLifecycle} currently defined for bean creation.
     *
     * @return the bean lifecycle currently defined
     */
    public ContextualLifecycle<T> getBeanLifecycle() {
        return beanLifecycle;
    }

    /**
     * Define the {@link ContextualLifecycle} used for bean creation.
     *
     * @param beanLifecycle the {@link ContextualLifecycle} to use for bean
     *                      creation.
     */
    public BeanBuilder<T> beanLifecycle(ContextualLifecycle<T> beanLifecycle) {
        this.beanLifecycle = beanLifecycle;
        return this;
    }

    /**
     * The bean class currently defined for bean creation.
     *
     * @return the bean class currently defined.
     */
    public Class<?> getBeanClass() {
        return beanClass;
    }

    /**
     * Define the bean class used for bean creation.
     *
     * @param beanClass the bean class to use
     */
    public BeanBuilder<T> beanClass(Class<?> beanClass) {
        this.beanClass = beanClass;
        return this;
    }

    /**
     * The bean manager in use. This cannot be changed for this
     * {@link BeanBuilder}.
     *
     * @return the bean manager in use
     */
    public BeanManager getBeanManager() {
        return beanManager;
    }

    /**
     * The name of the bean currently defined for bean creation.
     *
     * @return the name of the bean or <code>null</code> if the bean has no name
     */
    public String getName() {
        return name;
    }

    /**
     * Define the name of the bean used for bean creation.
     *
     * @param name the name of the bean to use or <code>null</code> if the bean
     *             should have no name
     */
    public BeanBuilder<T> name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Whether the created bean will be passivation capable.
     *
     * @return <code>true</code> if the created bean will be passivation capable,
     *         otherwise <code>false</code>
     */
    public boolean isPassivationCapable() {
        return passivationCapable;
    }

    /**
     * Define that the created bean will (or will not) be passivation capable.
     *
     * @param nullable <code>true</code> if the created bean should be
     *                 passivation capable, otherwise <code>false</code>
     */
    public BeanBuilder<T> passivationCapable(boolean passivationCapable) {
        this.passivationCapable = passivationCapable;
        return this;
    }

    /**
     * The id currently defined for bean creation.
     *
     * @return the id currently defined.
     */
    public String getId() {
        return id;
    }

    /**
     * Define the id used for bean creation.
     *
     * @param id the id to use
     */
    public BeanBuilder<T> id(String id) {
        this.id = id;
        return this;
    }

    /**
     * The injection points currently defined for bean creation.
     *
     * @return the injection points currently defined.
     */
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    /**
     * Define the injection points used for bean creation.
     *
     * @param injectionPoints the injection points to use
     */
    public BeanBuilder<T> injectionPoints(Set<InjectionPoint> injectionPoints) {
        this.injectionPoints = injectionPoints;
        return this;
    }

    /**
     * Define the string used when {@link #toString()} is called on the bean.
     *
     * @param toString the string to use
     */
    public BeanBuilder<T> toString(String toString) {
        this.toString = toString;
        return this;
    }

    /**
     * The string used when {@link #toString()} is called on the bean.
     *
     * @return the string currently defined
     */
    public String getToString() {
        return toString;
    }

}
