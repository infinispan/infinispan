package org.infinispan.query.impl;

import org.infinispan.metadata.Metadata;

public record EntityLoaded<E>(E entity, Metadata metadata) {

}
