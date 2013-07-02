package org.infinispan.query.impl;

import org.hibernate.search.query.engine.spi.EntityInfo;

import java.util.Collection;
import java.util.List;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public interface QueryResultLoader {

   Object load(EntityInfo entityInfo);

   List<Object> load(Collection<EntityInfo> entityInfos);

}
