package org.infinispan.cdi.common.util;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.util.AnnotationLiteral;

public class AnyLiteral extends AnnotationLiteral<Any> implements Any {
    private static final long serialVersionUID = -6858406907917381581L;

    public static final Any INSTANCE = new AnyLiteral();
}
