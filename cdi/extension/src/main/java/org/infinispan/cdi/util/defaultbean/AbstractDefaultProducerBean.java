package org.infinispan.cdi.util.defaultbean;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.infinispan.cdi.util.Beans;


/**
 * A helper class for implementing producer methods and fields on default beans
 *
 * @author Stuart Douglas
 */
abstract class AbstractDefaultProducerBean<T> extends AbstractDefaultBean<T> {

    private static final Annotation[] NO_QUALIFIERS = {};

    private final Type declaringBeanType;
    private final Annotation[] declaringBeanQualifiers;

    protected AbstractDefaultProducerBean(Bean<T> delegate, Type declaringBeanType, Set<Type> types, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, BeanManager beanManager) {
        super(delegate, declaringBeanType, types, qualifiers, beanManager);
        this.declaringBeanType = delegate.getBeanClass();
        this.declaringBeanQualifiers = declaringBeanQualifiers.toArray(NO_QUALIFIERS);
    }

    protected Annotation[] getDeclaringBeanQualifiers() {
        return declaringBeanQualifiers.clone();
    }

    protected Type getDeclaringBeanType() {
        return declaringBeanType;
    }

    protected abstract T getValue(Object receiver, CreationalContext<T> creationalContext);

    @Override
    public T create(CreationalContext<T> creationalContext) {
        Object receiver = getReceiver(creationalContext);
        T instance = getValue(receiver, creationalContext);
        Beans.checkReturnValue(instance, this, null, getBeanManager());
        return instance;
    }

    protected Object getReceiver(CreationalContext<T> creationalContext) {
        Bean<?> declaringBean = getBeanManager().resolve(getBeanManager().getBeans(getDeclaringBeanType(), declaringBeanQualifiers));
        return getBeanManager().getReference(declaringBean, declaringBean.getBeanClass(), creationalContext);
    }
}
