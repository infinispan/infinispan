/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
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

package org.jboss.as.clustering.infinispan;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * InfinispanLogger
 *
 * @author <a href="mailto:jperkins@redhat.com">James R. Perkins</a>
 * @author Tristan Tarrant
 */
@MessageLogger(projectCode = "DGISPN", length = 4)
public interface InfinispanLogger extends BasicLogger {
    String ROOT_LOGGER_CATEGORY = InfinispanLogger.class.getPackage().getName();

    /**
     * The root logger.
     */
    InfinispanLogger ROOT_LOGGER = Logger.getMessageLogger(InfinispanLogger.class, ROOT_LOGGER_CATEGORY);

    /**
     * Logs an informational message indicating the Infinispan subsystem is being activated.
     */
    @LogMessage(level = INFO)
    @Message(id = 0, value = "Activating Infinispan subsystem.")
    void activatingSubsystem();

    /**
     * Logs an informational message indicating that a cache is being started.
     *
     * @param cacheName     the name of the cache.
     * @param containerName the name of the cache container.
     */
    @LogMessage(level = INFO)
    @Message(id = 1, value = "Started %s cache from %s container")
    void cacheStarted(String cacheName, String containerName);


    /**
     * Logs an informational message indicating that a cache is being stopped.
     *
     * @param cacheName     the name of the cache.
     * @param containerName the name of the cache container.
     */
    @LogMessage(level = INFO)
    @Message(id = 2, value = "Stopped %s cache from %s container")
    void cacheStopped(String cacheName, String containerName);

    /**
     * Logs a warning message indicating that the eager attribute of the transactional element
     * is no longer valid
     */
    @LogMessage(level = WARN)
    @Message(id = 3, value = "The 'eager' attribute specified on the 'transaction' element of a cache is no longer valid")
    void eagerAttributeDeprecated();

    /**
     * Logs a warning message indicating that the specified topology attribute of the transport element
     * is no longer valid
     */
    @LogMessage(level = WARN)
    @Message(id = 4, value = "The '%s' attribute specified on the 'transport' element of a cache container is no longer valid" +
                "; use the same attribute specified on the 'transport' element of corresponding JGroups stack instead")
    void topologyAttributeDeprecated(String attribute);

    /**
     * Logs a debug message indicating that named cache container has been installed.
     */
    @LogMessage(level = DEBUG)
    @Message(id = 5, value = "'%s' cache container installed.")
    void cacheContainerInstalled(String containerName);

    /**
     * Logs a warning message stating that the 'virtual-nodes' attribute is deprecated.
     */
    @LogMessage(level = WARN)
    @Message(id = 6, value = "Attribute 'virtual-nodes' has been deprecated and has no effect.")
    void virtualNodesAttributeDeprecated();

   /**
    * Logs an info message about installing implementation service.
    */
    @LogMessage(level = INFO)
    @Message(id = 7, value = "Registering Deployed Cache Store service for store '%s'")
    void installDeployedCacheStore(String implementationClassName);

   /**
    * Logs debug message when starting Deployed Cache service.
    */
    @LogMessage(level = DEBUG)
    @Message(id = 8, value = "Started Deployed Cache service for implementation '%s'")
    void deployedStoreStarted(String className);

   /**
    * Logs debug message when stopping Deployed Cache service.
    */
    @LogMessage(level = DEBUG)
    @Message(id = 9, value = "Stopped Deployed Cache service for implementation '%s'")
    void deployedStoreStopped(String className);

    @LogMessage(level = WARN)
    @Message(id = 10, value = "The '%s' attribute has been deprecated and is now ignored. Please use the '%s' configuration element instead")
    void deprecatedExecutor(String executorAttribute, String threadPoolElement);

    /**
     * Logs an info message about installing implementation service.
     */
    @LogMessage(level = INFO)
    @Message(id = 11, value = "Installing ServerTask service implementation '%s'")
    void installingDeployedTaskService(String implementationClassName);

    /**
     * Logs debug message when starting deployed task.
     */
    @LogMessage(level = DEBUG)
    @Message(id = 12, value = "Registering task '%s'")
    void registeringDeployedTask(String className);

    /**
     * Logs debug message when stopping deployed task.
     */
    @LogMessage(level = DEBUG)
    @Message(id = 13, value = "Unregistering task '%s'")
    void unregisteringDeployedTask(String className);

    /**
     * Logs a warning message indicating that the flush-lock-timeout attribute of the write-behind element
     * is no longer valid
     */
    @LogMessage(level = WARN)
    @Message(id = 14, value = "The 'flush-lock-timeout' attribute specified on the 'write-behind' element of a cache is no longer valid")
    void flushLockTimeoutDeprecated();

    /**
     * Logs a warning message indicating that the flush-lock-timeout attribute of the write-behind element
     * is no longer valid
     */
    @LogMessage(level = WARN)
    @Message(id = 15, value = "The 'shutdown-timeout' attribute specified on the 'write-behind' element of a cache is no longer valid")
    void shutdownTimeoutDeprecated();

    /**
     * Logs an error when requested cache store is not loaded within 1 minute
     */
    @LogMessage(level = ERROR)
    @Message(id = 16, value = "Waiting for deployment of Custom Cache Store (%s) timed out. Please check if this cache store is really present.")
    void loadingCustomCacheStoreTimeout(String customStoreClassName);

    /**
     * Logs a warning message indicating that the module attribute of the cache element is no longer valid.
     */
    @LogMessage(level = WARN)
    @Message(id = 17, value = "Found and ignored unsupported (deprecated) attribute 'module' in cache configuration at [row,col] [%s, %s]")
    void cacheModuleDeprecated(int row, int col);

    @LogMessage(level = INFO)
    @Message(id = 18, value = "Registering custom EntryMergePolicy '%s'")
    void registeringCustomMergePolicy(String className);

    @LogMessage(level = INFO)
    @Message(id = 19, value = "Unregistering custom EntryMergePolicy '%s'")
    void unregisteringCustomMergePolicy(String className);

    @LogMessage(level = ERROR)
    @Message(id = 20, value = "Waiting for deployment of custom EntryMergePolicy (%s) timed out. Please check if this EntryMergePolicy is really present.")
    void loadingCustomMergePolicyTimeout(String className);

    @LogMessage(level = WARN)
    @Message(id = 21, value = "Managed configuration storage is currently unsupported in domain mode. Please use Overlay storage.")
    void managedConfigurationUnavailableInDomainMode();
}
