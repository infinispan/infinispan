package org.infinispan.server.infinispan.task;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;


/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/22/16
 * Time: 2:21 PM
 */
@MetaInfServices
@SuppressWarnings("unused")
public class InfinispanIntegrationMetadataFileFinder implements ModuleMetadataFileFinder {

   @Override
   public String getMetadataFilename() {
      return "infinispan-server-infinispan-component-metadata.dat";
   }
}
