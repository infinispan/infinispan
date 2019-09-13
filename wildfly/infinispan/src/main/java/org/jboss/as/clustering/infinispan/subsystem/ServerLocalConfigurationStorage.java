package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.globalstate.LocalConfigurationStorage;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.client.ModelControllerClient;

/**
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface ServerLocalConfigurationStorage extends LocalConfigurationStorage {

   void setRootPath(PathAddress rootPath);

   void setModelControllerClient(ModelControllerClient modelControllerClient);
}
