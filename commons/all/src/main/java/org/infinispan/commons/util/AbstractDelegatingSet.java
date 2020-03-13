package org.infinispan.commons.util;

import java.util.Set;

public abstract class AbstractDelegatingSet<E> extends AbstractDelegatingCollection<E> implements Set<E> {

   protected abstract Set<E> delegate();
}
