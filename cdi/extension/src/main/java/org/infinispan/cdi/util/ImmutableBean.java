package org.infinispan.cdi.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

import javax.enterprise.context.Dependent;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.InjectionPoint;

/**
 * <p>
 * A base class for implementing {@link Bean}. The attributes are immutable, and
 * collections are defensively copied on instantiation. It uses the defaults
 * from the specification for properties if not specified.
 * </p>
 * <p/>
 * <p>
 * This bean delegates it's lifecycle to the callbacks on the provided
 * {@link ContextualLifecycle}.
 * </p>
 *
 * @author Stuart Douglas
 * @author Pete Muir
 * @see AbstractImmutableBean
 * @see BeanBuilder
 * @see ImmutablePassivationCapableBean
 */
public class ImmutableBean<T> extends AbstractImmutableBean<T> implements Bean<T> {

    private final ContextualLifecycle<T> contextualLifecycle;

    /**
     * Create a new, immutable bean. All arguments passed as collections are
     * defensively copied.
     *
     * @param beanClass           The Bean class, may not be null
     * @param name                The bean name
     * @param qualifiers          The bean's qualifiers, if null, a singleton set of
     *                            {@link Default} is used
     * @param scope               The bean's scope, if null, the default scope of
     *                            {@link Dependent} is used
     * @param stereotypes         The bean's stereotypes, if null, an empty set is used
     * @param types               The bean's types, if null, the beanClass and {@link Object}
     *                            will be used
     * @param alternative         True if the bean is an alternative
     * @param nullable            True if the bean is nullable
     * @param injectionPoints     the bean's injection points, if null an empty set
     *                            is used
     * @param contextualLifecycle Handler for {@link #create(CreationalContext)}
     *                            and {@link #destroy(Object, CreationalContext)}
     * @param toString            the string representation of the bean, if null the built
     *                            in representation is used, which states the bean class and
     *                            qualifiers
     * @throws IllegalArgumentException if the beanClass is null
     */
    public ImmutableBean(Class<?> beanClass, String name, Set<Annotation> qualifiers, Class<? extends Annotation> scope, Set<Class<? extends Annotation>> stereotypes, Set<Type> types, boolean alternative, boolean nullable, Set<InjectionPoint> injectionPoints, ContextualLifecycle<T> contextualLifecycle, String toString) {
        super(beanClass, name, qualifiers, scope, stereotypes, types, alternative, nullable, injectionPoints, toString);
        this.contextualLifecycle = contextualLifecycle;
    }

    public T create(CreationalContext<T> arg0) {
        return contextualLifecycle.create(this, arg0);
    }

    public void destroy(T arg0, CreationalContext<T> arg1) {
        contextualLifecycle.destroy(this, arg0, arg1);
    }

}
