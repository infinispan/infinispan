package org.infinispan.cdi.common.util.annotatedtypebuilder;

import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.Type;

/**
 * @author Stuart Douglas
 */
class AnnotatedFieldImpl<X> extends AnnotatedMemberImpl<X, Field> implements AnnotatedField<X> {

    AnnotatedFieldImpl(AnnotatedType<X> declaringType, Field field, AnnotationStore annotations, Type overridenType) {
        super(declaringType, field, field.getType(), annotations, field.getGenericType(), overridenType);
    }

}
