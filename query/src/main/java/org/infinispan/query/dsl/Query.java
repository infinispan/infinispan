package org.infinispan.query.dsl;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Query {

   <T> List<T> list();
}
