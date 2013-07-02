package org.infinispan.test.fwk;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Indicates that test cleanup happens after all test methods.
 *
 * @author Manik Surtani
 * @version 4.2
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CleanupAfterTest {
}
