package org.infinispan.commons;

import org.infinispan.commons.executors.BlockingResource;
import org.infinispan.commons.executors.NonBlockingResource;

public interface ThreadGroups {

    ISPNNonBlockingThreadGroup NON_BLOCKING_GROUP = new ISPNNonBlockingThreadGroup("ISPN-non-blocking-group");
    ISPNBlockingThreadGroup BLOCKING_GROUP = new ISPNBlockingThreadGroup("ISPN-blocking-group");

    final class ISPNNonBlockingThreadGroup extends ThreadGroup implements NonBlockingResource {
        public ISPNNonBlockingThreadGroup(String name) {
            super(name);
        }
    }

    final class ISPNBlockingThreadGroup extends ThreadGroup implements BlockingResource {
        public ISPNBlockingThreadGroup(String name) {
            super(name);
        }
    }
}
