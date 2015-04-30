package org.infinispan.query.impl;

import java.util.List;

import org.hibernate.search.query.engine.spi.EntityInfo;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public interface QueryResultLoader {

   Object load(EntityInfo entityInfo);

   List<Object> load(List<EntityInfo> entityInfos);

}
