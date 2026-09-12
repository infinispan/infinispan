package org.infinispan.protostream.sampledomain;

import org.infinispan.protostream.annotations.ProtoField;

public record NonIndexedGame(@ProtoField(1) String name, @ProtoField(2) String description) {
}
