package org.infinispan.server.test.util;

import org.apache.log4j.Logger;
import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

/**
 * Logs test methods.
 * 
 * @author <a href="mailto:mlinhard@redhat.com">Michal Linhard</a>
 *
 */
public class TestsuiteListener extends RunListener {
    
    private static final Logger log = Logger.getLogger(TestsuiteListener.class);

    @Override
    public void testStarted(Description description) throws Exception {
        super.testStarted(description);
        log.info("Running " + description.getClassName() + "#" + description.getMethodName());
    }
}
