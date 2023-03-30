package org.infinispan.quarkus.embedded.runtime.graal;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.RecomputeFieldValue;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import org.jgroups.JChannel;
import org.jgroups.protocols.DELAY;
import org.jgroups.util.Util;

import java.util.Random;

public class SubstituteJGroups {
}

@TargetClass(DELAY.class)
final class SubstitueDELAY {
    @Alias
    // Force it to null - so it can be reinitialized
    @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
    protected static volatile Random randomNumberGenerator;
}

// DISCARD protocol uses swing classes
@TargetClass(className = "org.jgroups.protocols.DISCARD")
final class SubstituteDiscardProtocol {

    @Substitute
    public void startGui() {
        // do nothing
    }

    @Substitute
    public void stopGui() {
        // do nothing
    }

    @Substitute
    public void start() throws Exception {
        // should call super.start() but the "super" Protocol.start() does nothing,
        // so this empty impl is OK
    }

    @Substitute
    public void stop() {
        // should call super.stop() but the "super" Protocol.stop() does nothing,
        // so this empty impl is OK

    }
}

@TargetClass(Util.class)
final class SubstituteJgroupsUtil {

    @Substitute
    public static void registerChannel(JChannel channel, String name) {
        // do nothing
    }
}
