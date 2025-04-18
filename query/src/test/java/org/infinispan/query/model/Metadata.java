package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Metadata {
    private final String key;
    private final String value;

    @ProtoFactory
    public Metadata(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Basic(projectable = true)
    @ProtoField(1)
    public String getKey() {
        return key;
    }

    @Basic(projectable = true)
    @ProtoField(2)
    public String getValue() {
        return value;
    }
}
