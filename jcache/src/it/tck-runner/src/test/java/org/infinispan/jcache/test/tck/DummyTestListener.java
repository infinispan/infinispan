package org.infinispan.jcache.test.tck;

import org.junit.runner.notification.RunListener;

/**
 * Plug dummy test listener to TCK testsuite so that Infinispan's TestNG
 * listener is not used and avoid dependency on core test jar.
 *
 * @author Galder Zamarreño
 */
public class DummyTestListener extends RunListener {

}
