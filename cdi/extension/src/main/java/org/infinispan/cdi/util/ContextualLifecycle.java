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
