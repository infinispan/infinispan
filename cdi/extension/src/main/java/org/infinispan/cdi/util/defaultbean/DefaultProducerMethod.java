package org.infinispan.cdi.util.defaultbean;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.infinispan.cdi.util.InjectableMethod;

// TODO Make this passivation capable
class DefaultProducerMethod<T, X> extends AbstractDefaultProducerBean<T> {

    private final InjectableMethod<X> producerMethod;
    private final InjectableMethod<X> disposerMethod;

    static <T, X> DefaultProducerMethod<T, X> of(Bean<T> originalBean, Type declaringBeanType, Set<Type> beanTypes, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, AnnotatedMethod<X> method, AnnotatedMethod<X> disposerMethod, BeanManager beanManager) {
        return new DefaultProducerMethod<T, X>(originalBean, declaringBeanType, beanTypes, qualifiers, declaringBeanQualifiers, method, disposerMethod, beanManager);
    }

    DefaultProducerMethod(Bean<T> originalBean, Type declaringBeanType, Set<Type> beanTypes, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, AnnotatedMethod<X> method, AnnotatedMethod<X> disposerMethod, BeanManager beanManager) {
        super(originalBean, declaringBeanType, beanTypes, qualifiers, declaringBeanQualifiers, beanManager);
        this.producerMethod = new InjectableMethod<X>(method, this, beanManager);
        if (disposerMethod != null) {
            this.disposerMethod = new InjectableMethod<X>(disposerMethod, this, beanManager);
        } else {
            this.disposerMethod = null;
        }
    }

    @Override
    protected T getValue(Object receiver, CreationalContext<T> creationalContext) {
        return producerMethod.invoke(receiver, creationalContext);
    }

    @Override
    public void destroy(T instance, CreationalContext<T> creationalContext) {
        if (disposerMethod != null) {
            try {
                disposerMethod.invoke(getReceiver(creationalContext), creationalContext);
            } finally {
                creationalContext.release();
            }
        }
    }

}
