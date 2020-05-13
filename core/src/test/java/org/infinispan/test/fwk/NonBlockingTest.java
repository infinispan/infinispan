package org.infinispan.test.fwk;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Indicates that this test invokes methods that should be all non blocking. If any such method is invoked the test
 * will fail and report a blocking issue. Note that the non blocking is only verified for invoked tests. Setup and tear
 * down methods are not affected.
 *
 * @author William Burns
 * @version 11.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface NonBlockingTest {
}
