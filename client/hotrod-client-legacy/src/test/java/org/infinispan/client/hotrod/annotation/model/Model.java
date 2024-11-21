package org.infinispan.client.hotrod.annotation.model;

public interface Model {
    default String getId() {
        return null;
    }
}
