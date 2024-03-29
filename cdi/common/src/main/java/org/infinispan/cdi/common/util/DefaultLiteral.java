package org.infinispan.cdi.common.util;

import jakarta.enterprise.inject.Default;
import jakarta.enterprise.util.AnnotationLiteral;

public class DefaultLiteral extends AnnotationLiteral<Default> implements Default {
    private static final long serialVersionUID = -8137340248362361317L;

    public static final Default INSTANCE = new DefaultLiteral();
}
