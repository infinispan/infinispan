package org.infinispan.cdi.util.defaultbean;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.PassivationCapable;

import org.infinispan.cdi.util.ForwardingBean;

/**
 * A helper class for implementing default bean functionality
 *
 * @author Stuart Douglas
 */
abstract class AbstractDefaultBean<T> extends ForwardingBean<T> implements PassivationCapable {

    private final Bean<T> delegate;
    private final Set<Annotation> qualifiers;
    private final Set<Type> types;
    private final BeanManager beanManager;
    private final Type declaringBeanType;

    protected AbstractDefaultBean(Bean<T> delegate, Type declaringBeanType, Set<Type> types, Set<Annotation> qualifiers, BeanManager beanManager) {
        this.delegate = delegate;
        this.beanManager = beanManager;
        this.qualifiers = new HashSet<Annotation>(qualifiers);
        this.types = new HashSet<Type>(types);
        this.declaringBeanType = declaringBeanType;
    }

    protected BeanManager getBeanManager() {
        return beanManager;
    }

    @Override
    protected Bean<T> delegate() {
        return delegate;
    }

    protected Type getDeclaringBeanType() {
        return declaringBeanType;
    }

    @Override
    public Set<Annotation> getQualifiers() {
        return qualifiers;
    }

    public String getId() {
        if (delegate instanceof PassivationCapable) {
            return DefaultBean.class.getName() + "-" + ((PassivationCapable) delegate).getId();
        }
        return DefaultBean.class.getName() + "-" + types.toString() + "-QUALIFIERS-" + delegate.getQualifiers().toString();
    }

    @Override
    public String toString() {
        return "Default Bean with types " + types + " and qualifiers " + qualifiers;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((qualifiers == null) ? 0 : qualifiers.hashCode());
        result = prime * result + ((types == null) ? 0 : types.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (getClass() != obj.getClass())
            return false;
        AbstractDefaultBean<?> other = (AbstractDefaultBean<?>) obj;
        if (qualifiers == null) {
            if (other.qualifiers != null)
                return false;
        } else if (!qualifiers.equals(other.qualifiers))
            return false;
        if (types == null) {
            if (other.types != null)
                return false;
        } else if (!types.equals(other.types))
            return false;
        return delegate.equals(other.delegate);
    }
}
