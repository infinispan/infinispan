/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.rhq.logging;

import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;
import org.mc4j.ems.connection.bean.EmsBean;

import java.util.List;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the RHQ plugin. For this module, message ids
 * ranging from 16001 to 17000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = WARN)
   @Message(value = "Attribute %s not found", id = 16001)
   void attributeNotFound(String attributeName);

   @LogMessage(level = WARN)
   @Message(value = "getValues failed for %s", id = 16002)
   void getValuesFailed(String metric, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unknown %s attribute type for %s", id = 16003)
   void unknownAttributeType(Class attrType, Object o);

   @LogMessage(level = WARN)
   @Message(value = "More than one bean returned from applying %s pattern: %s", id = 16004)
   void moreThanOneBeanReturned(String pattern, List<EmsBean> beans);

   @LogMessage(level = INFO)
   @Message(value = "Discovered Infinispan instance with key %s and name %s", id = 16005)
   void discoveredInfinispanInstance(String resourceName, String managerName);


}
