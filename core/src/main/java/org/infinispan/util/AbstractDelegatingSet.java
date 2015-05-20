package org.infinispan.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public abstract class AbstractDelegatingSet<E> extends AbstractDelegatingCollection<E> implements Set<E> {

   protected abstract Set<E> delegate();
}
