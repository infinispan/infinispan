package org.infinispan.cli.completers;

import org.infinispan.configuration.cache.XSiteStateTransferMode;

/**
 * An {@link EnumCompleter} implementation for {@link XSiteStateTransferMode}
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class XSiteStateTransferModeCompleter extends EnumCompleter<XSiteStateTransferMode> {

   public XSiteStateTransferModeCompleter() {
      super(XSiteStateTransferMode.class);
   }
}
