package org.infinispan.client.hotrod.query.key.tutorial;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.Proto;

@Proto
@Indexed
public record PersonKey(
      @Basic(projectable = true)
      String id,
      @Basic(projectable = true)
      String pseudo,
      @Embedded
      Author author
) {
}
