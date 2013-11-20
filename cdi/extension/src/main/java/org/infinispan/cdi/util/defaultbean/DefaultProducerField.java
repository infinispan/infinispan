package org.infinispan.cdi.util.defaultbean;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.infinispan.cdi.util.Reflections;

// TODO Make this passivation capable
class DefaultProducerField<T, X> extends AbstractDefaultProducerBean<T> {

    private final AnnotatedField<X> field;

    static <T, X> DefaultProducerField<T, X> of(Bean<T> originalBean, Type declaringBeanType, Set<Type> beanTypes, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, AnnotatedField<X> field, BeanManager beanManager) {
        return new DefaultProducerField<T, X>(originalBean, declaringBeanType, beanTypes, qualifiers, declaringBeanQualifiers, field, beanManager);
    }

    DefaultProducerField(Bean<T> originalBean, Type declaringBeanType, Set<Type> beanTypes, Set<Annotation> qualifiers, Set<Annotation> declaringBeanQualifiers, AnnotatedField<X> field, BeanManager beanManager) {
        super(originalBean, declaringBeanType, beanTypes, qualifiers, declaringBeanQualifiers, beanManager);
        this.field = field;
        if (!field.getJavaMember().isAccessible()) {
            field.getJavaMember().setAccessible(true);
        }
    }

    @Override
    protected T getValue(Object receiver, CreationalContext<T> creationalContext) {
        return Reflections.getFieldValue(field.getJavaMember(), receiver, Reflections.<T>getRawType(field.getBaseType()));
    }

    @Override
    public void destroy(T instance, CreationalContext<T> creationalContext) {
        // TODO: disposers
        creationalContext.release();
    }

}
