package org.infinispan.quarkus.embedded.runtime.graal;

import org.infinispan.quarkus.embedded.runtime.Util;
import org.jgroups.Address;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

/**
 * These substitutions need to be revisited on each new Quarkus/GraalVM release, as some methods may become
 * supported.
 *
 * @author Bela Ban
 * @since 1.0.0
 */
class JChannelSubstitutions {
}

@TargetClass(className = "org.jgroups.protocols.VERIFY_SUSPECT")
final class Target_org_jgroups_protocols_VERIFY_SUSPECT {

    @Substitute
    protected void verifySuspectWithICMP(Address suspected_mbr) {
        throw Util.unsupportedOperationException("VERIFY_SUSPECT Protocol");
    }
}
