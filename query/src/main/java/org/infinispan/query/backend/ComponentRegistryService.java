package org.infinispan.query.backend;

import org.hibernate.search.engine.service.spi.Service;
import org.infinispan.factories.ComponentRegistry;

public interface ComponentRegistryService extends Service {

   ComponentRegistry getComponentRegistry();

}