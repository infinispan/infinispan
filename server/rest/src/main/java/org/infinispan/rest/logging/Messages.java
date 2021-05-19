package org.infinispan.rest.logging;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Informational Scripting messages. These start from 12500 so as not to overlap with the logging
 * messages defined in {@link Log} Messages.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(Messages.class);

   @Message(value = "Reject rule '%s' matches request address '%s'", id = 12500)
   String rejectRuleMatchesRequestAddress(Object rule, Object address);

   @Message(value = "Request to stop connector '%s' attempted from the connector itself", id = 12501)
   String connectorMatchesRequest(String connectorName);

   @Message(value = "Connection to data source '%s' successful", id = 12502)
   String dataSourceTestOk(String name);

   @Message(value = "Connection to data source '%s' failed. Verify that the JDBC URL and credentials are correct and that the driver is present in the server library directory", id = 12503)
   String dataSourceTestFail(String name);
}
