package org.infinispan.cdi.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

/**
 * Represents a non-contextual instance
 */
public class ContextualReference<T> {

    private final Bean<T> bean;
    private T instance;
    private boolean disposed = false;

    public ContextualReference(BeanManager beanManager, Type beantype, Annotation... qualifiers) {
        this.bean = (Bean<T>) beanManager.resolve(beanManager.getBeans(beantype, qualifiers));
    }

    /**
     * Get the instance
     */
    public T get() {
        return instance;
    }

    /**
     * Create the instance
     */
    public ContextualReference<T> create(CreationalContext<T> ctx) {
        if (this.instance != null) {
            throw new IllegalStateException("Trying to call create() on already constructed instance");
        }
        if (disposed) {
            throw new IllegalStateException("Trying to call create() on an already disposed instance");
        }
        this.instance = bean.create(ctx);
        return this;
    }

    /**
     * destroy the bean
     */
    public ContextualReference<T> destroy(CreationalContext<T> ctx) {
        if (this.instance == null) {
            throw new IllegalStateException("Trying to call destroy() before create() was called");
        }
        if (disposed) {
            throw new IllegalStateException("Trying to call destroy() on already disposed instance");
        }
        this.disposed = true;
        bean.destroy(instance, ctx);
        return this;
    }

}