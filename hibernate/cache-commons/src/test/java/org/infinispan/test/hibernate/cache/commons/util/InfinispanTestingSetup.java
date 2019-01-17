/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.util;

import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * TestRule that sets the test name so that Infinispan test utilities can track the shutdown of cache managers and thread leaks.
 *
 * <p>Use as a {@link ClassRule} or {@link Rule}.</p>
 *
 * <p>Note: {@code BaseUnitTestCase} uses a {@code Timeout} rule that runs each method in a separate thread,
 * so tests extending {@code BaseUnitTestCase} must use {@link Rule} or invoke {@link #joinContext()}
 * in the test methods.</p>
 *
 * @author Sanne Grinovero
 */
public final class InfinispanTestingSetup implements TestRule {

    private volatile String runningTest;

    public InfinispanTestingSetup() {
    }

    public Statement apply(Statement base, Description d) {
        final String methodName = d.getMethodName();
        final String testName = methodName == null ? d.getClassName() : d.getClassName() + "#" + d.getMethodName();
        runningTest = testName;
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                TestResourceTracker.testStarted( testName );
                try {
                    base.evaluate();
                } finally {
                    TestResourceTracker.testFinished( testName );
                }
            }
        };
    }

   /**
    * Make a new thread join the test context.
    */
   public void joinContext() {
        TestResourceTracker.setThreadTestName( runningTest );
    }

}
