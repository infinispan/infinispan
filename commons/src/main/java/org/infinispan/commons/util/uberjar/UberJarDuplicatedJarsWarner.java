package org.infinispan.commons.util.uberjar;

import java.util.concurrent.CompletableFuture;

/**
 * Checks if classpath contains proper configuration for Uber Jars and warns if it does not.
 *
 * @author slaskawi
 */
public interface UberJarDuplicatedJarsWarner {

    /**
     * Synchronously checks if classpath looks correct for Uber Jar usage.
     *
     * @return <code>true</code> if duplicate is found.
     */
    boolean isClasspathCorrect();

    /**
     * Asynchronously checks if classpath looks correct for Uber Jar usage.
     *
     * @return {@link CompletableFuture} with the result.
     */
    CompletableFuture<Boolean> isClasspathCorrectAsync();
}
