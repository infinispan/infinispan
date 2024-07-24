package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.Proto;

@Proto
@Indexed // @Indexed: Workaround for https://issues.redhat.com/browse/ISPN-16314
public record Player(@Basic String name, @Basic String color, @Basic Integer number) {

}
