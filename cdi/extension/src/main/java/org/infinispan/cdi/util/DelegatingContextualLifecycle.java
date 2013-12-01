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
