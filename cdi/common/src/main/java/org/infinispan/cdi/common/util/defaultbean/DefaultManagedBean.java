package org.infinispan.cdi.common.util.defaultbean;

import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

class DefaultManagedBean<T> extends AbstractDefaultBean<T> {

    static <T> DefaultManagedBean<T> of(Bean<T> originalBean, Type defaultBeanType, Set<Type> types, Set<Annotation> qualifiers, BeanManager beanManager) {
        return new DefaultManagedBean<T>(originalBean, defaultBeanType, types, qualifiers, beanManager);
    }

    DefaultManagedBean(Bean<T> originalBean, Type defaultBeanType, Set<Type> types, Set<Annotation> qualifiers, BeanManager beanManager) {
        super(originalBean, defaultBeanType, types, qualifiers, beanManager);
    }

}
