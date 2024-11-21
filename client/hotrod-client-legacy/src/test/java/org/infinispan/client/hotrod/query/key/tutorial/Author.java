package org.infinispan.client.hotrod.query.key.tutorial;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.Proto;

@Proto
@Indexed
public record Author(
      @Basic(projectable=true)
      String id,
      @Basic(projectable=true)
      String name
) {
}
