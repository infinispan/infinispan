package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.protostream.annotations.Proto;

@Proto
public record Player(@Basic String name, @Basic String color, @Basic Integer number) {

}
