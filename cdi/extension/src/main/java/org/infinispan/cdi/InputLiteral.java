package org.infinispan.cdi;

import javax.enterprise.inject.Default;
import javax.enterprise.util.AnnotationLiteral;

/**
 * Annotation literal for {@link Input}
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@SuppressWarnings("all")
public class InputLiteral extends AnnotationLiteral<Input> implements Input {


    /** The serialVersionUID */
   private static final long serialVersionUID = -6499058493830063773L;

}