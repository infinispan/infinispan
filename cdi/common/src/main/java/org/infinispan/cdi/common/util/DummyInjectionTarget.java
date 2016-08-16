package org.infinispan.cdi.common.util;

import static java.util.Collections.emptySet;

import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;

/**
 * Injection target implementation that does nothing
 *
 * @author Stuart Douglas
 */
public class DummyInjectionTarget<T> implements InjectionTarget<T> {

    public void inject(T instance, CreationalContext<T> ctx) {
    }

    public void postConstruct(T instance) {
    }

    public void preDestroy(T instance) {
    }

    public void dispose(T instance) {
    }

    public Set<InjectionPoint> getInjectionPoints() {
        return emptySet();
    }

    public T produce(CreationalContext<T> ctx) {
        return null;
    }

}
