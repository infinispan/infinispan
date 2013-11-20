package org.infinispan.cdi.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

import javax.enterprise.event.Reception;
import javax.enterprise.event.TransactionPhase;
import javax.enterprise.inject.spi.ObserverMethod;

/**
 * An implementation of {@link ObserverMethod} that forwards all calls to
 * {@link #delegate()}.
 *
 * @param <T> The event type
 * @author Pete Muir
 */
public abstract class ForwardingObserverMethod<T> implements ObserverMethod<T> {

    /**
     * All calls to this {@link ObserverMethod} instance are forwarded to the
     * delegate unless overridden.
     *
     * @return the delegate {@link ObserverMethod}
     */
    protected abstract ObserverMethod<T> delegate();

    public Class<?> getBeanClass() {
        return delegate().getBeanClass();
    }

    public Set<Annotation> getObservedQualifiers() {
        return delegate().getObservedQualifiers();
    }

    public Type getObservedType() {
        return delegate().getObservedType();
    }

    public Reception getReception() {
        return delegate().getReception();
    }

    public TransactionPhase getTransactionPhase() {
        return delegate().getTransactionPhase();
    }

    public void notify(T event) {
        delegate().notify(event);
    }

    @Override
    public boolean equals(Object obj) {
        return delegate().equals(obj);
    }

    @Override
    public int hashCode() {
        return delegate().hashCode();
    }

    @Override
    public String toString() {
        return delegate().toString();
    }

}
