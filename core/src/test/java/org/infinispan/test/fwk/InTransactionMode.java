package org.infinispan.test.fwk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.transaction.TransactionMode;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface InTransactionMode {
   TransactionMode[] value();
}
