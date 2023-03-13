package org.infinispan.quarkus.server.runtime.graal;

import java.util.Collections;
import java.util.List;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.RecomputeFieldValue;
import com.oracle.svm.core.annotate.TargetClass;

public class FixInetAddress {
}

@TargetClass(org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask.class)
final class MultiHomedServerAddress {
   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
   static org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask RFC1918_CIDR_10;

   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
   static org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask RFC1918_CIDR_172;

   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
   static org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask RFC1918_CIDR_192;

   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
   static org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask RFC3927_LINK_LOCAL;

   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
   static org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask RFC1112_RESERVED;

   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
   static org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask RFC6598_SHARED_SPACE;

   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
   static org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask RFC4193_ULA;

   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.Reset)
   static org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask RFC4193_LINK_LOCAL;

   @Alias
   @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.FromAlias)
   static List<org.infinispan.server.hotrod.MultiHomedServerAddress.InetAddressWithNetMask> PRIVATE_NETWORKS = Collections.emptyList();
}
