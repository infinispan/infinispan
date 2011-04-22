<?xml version="1.0" encoding="UTF-8"?>

<!--
  ~ JBoss, Home of Professional Open Source
  ~ Copyright 2009 Red Hat Inc. and/or its affiliates and other
  ~ contributors as indicated by the @author tags. All rights reserved.
  ~ See the copyright.txt in the distribution for a full listing of
  ~ individual contributors.
  ~
  ~ This is free software; you can redistribute it and/or modify it
  ~ under the terms of the GNU Lesser General Public License as
  ~ published by the Free Software Foundation; either version 2.1 of
  ~ the License, or (at your option) any later version.
  ~
  ~ This software is distributed in the hope that it will be useful,
  ~ but WITHOUT ANY WARRANTY; without even the implied warranty of
  ~ MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  ~ Lesser General Public License for more details.
  ~
  ~ You should have received a copy of the GNU Lesser General Public
  ~ License along with this software; if not, write to the Free
  ~ Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  ~ 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  -->
<xsl:stylesheet xmlns="urn:infinispan:config:5.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
   <xsl:output method="xml" indent="yes" version="1.0" encoding="UTF-8" omit-xml-declaration="no"/>

   <xsl:template match="/jbosscache">

      <xsl:element name="infinispan">
         <xsl:element name="global">
            <xsl:element name="asyncListenerExecutor">
               <xsl:attribute name="factory">org.infinispan.executors.DefaultExecutorFactory</xsl:attribute>
               <properties>
                  <xsl:if test="listeners[@asyncPoolSize]">
                     <xsl:element name="property">
                        <xsl:attribute name="name">maxThreads</xsl:attribute>
                        <xsl:attribute name="value">
                           <xsl:value-of select="normalize-space(listeners/@asyncPoolSize)"/>
                        </xsl:attribute>
                     </xsl:element>
                  </xsl:if>
                  <xsl:if test="listeners[@asyncQueueSize]">
                     <xsl:element name="property">
                        <xsl:attribute name="name">queueSize</xsl:attribute>
                        <xsl:attribute name="value">
                           <xsl:value-of select="listeners/@asyncQueueSize"/>
                        </xsl:attribute>
                     </xsl:element>
                  </xsl:if>
                  <property name="threadNamePrefix" value="AsyncListenerThread"/>
               </properties>
            </xsl:element>

            <xsl:element name="asyncTransportExecutor">
               <xsl:attribute name="factory">org.infinispan.executors.DefaultExecutorFactory</xsl:attribute>
               <properties>
                  <xsl:if test="clustering/async[@serializationExecutorPoolSize]">
                     <xsl:element name="property">
                        <xsl:attribute name="name">maxThreads</xsl:attribute>
                        <xsl:attribute name="value">
                           <xsl:value-of select="clustering/async/@serializationExecutorPoolSize"/>
                        </xsl:attribute>
                     </xsl:element>
                  </xsl:if>
                  <xsl:if test="clustering/async[@serializationExecutorQueueSize]">
                     <xsl:element name="property">
                        <xsl:attribute name="name">queueSize</xsl:attribute>
                        <xsl:attribute name="value">
                           <xsl:value-of select="clustering/async/@serializationExecutorQueueSize"/>
                        </xsl:attribute>
                     </xsl:element>
                  </xsl:if>
                  <property name="threadNamePrefix" value="AsyncSerializationThread"/>
               </properties>
            </xsl:element>

            <evictionScheduledExecutor factory="org.infinispan.executors.DefaultScheduledExecutorFactory">
               <properties>
                  <property name="threadNamePrefix" value="EvictionThread"/>
               </properties>
            </evictionScheduledExecutor>

            <replicationQueueScheduledExecutor factory="org.infinispan.executors.DefaultScheduledExecutorFactory">
               <properties>
                  <property name="threadNamePrefix" value="ReplicationQueueThread"/>
               </properties>
            </replicationQueueScheduledExecutor>

            <xsl:element name="globalJmxStatistics">
               <xsl:attribute name="jmxDomain">infinispan</xsl:attribute>
               <xsl:if test="jmxStatistics[@enabled]">
                  <xsl:attribute name="enabled">
                     <xsl:value-of select="jmxStatistics/@enabled"/>
                  </xsl:attribute>
               </xsl:if>
            </xsl:element>

            <xsl:element name="transport">
               <xsl:attribute name="transportClass">org.infinispan.remoting.transport.jgroups.JGroupsTransport</xsl:attribute>
               <xsl:if test="clustering[@clusterName]">
                  <xsl:attribute name="clusterName">
                     <xsl:value-of select="clustering/@clusterName"/>
                  </xsl:attribute>
               </xsl:if>
               <xsl:if test="clustering/sync[@replTimeout]"> <!-- this defaults to 6000 -->
                  <xsl:attribute name="distributedSyncTimeout">
                     <xsl:value-of select="clustering/sync/@replTimeout"/>
                  </xsl:attribute>
               </xsl:if>
               <xsl:element name="properties">
                  <xsl:if test="clustering/jgroupsConfig[@configFile]">
                     <xsl:element name="property">
                        <xsl:attribute name="name">configurationFile</xsl:attribute>
                        <xsl:attribute name="value">
                           <xsl:value-of select="clustering/jgroupsConfig/@configFile"/>
                        </xsl:attribute>
                     </xsl:element>
                  </xsl:if>
                  <xsl:if test="clustering/jgroupsConfig/*">
                     <xsl:element name="property">
                        <xsl:attribute name="name">configurationFile</xsl:attribute>
                        <xsl:attribute name="value">jgroupsConfig.xml</xsl:attribute>
                     </xsl:element>
                  </xsl:if>
               </xsl:element>

            </xsl:element>

            <xsl:if test="shutdown[@hookBehavior]">
               <xsl:element name="shutdown">
                  <xsl:attribute name="hookBehavior">
                     <xsl:value-of select="shutdown/@hookBehavior"/>
                  </xsl:attribute>
               </xsl:element>
            </xsl:if>
         </xsl:element>

         <default>

            <xsl:if test="locking">
               <xsl:element name="locking">
                  <xsl:if test="locking[@isolationLevel]">
                     <xsl:attribute name="isolationLevel">
                        <xsl:value-of select="locking/@isolationLevel"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="locking[@lockAcquisitionTimeout]">
                     <xsl:attribute name="lockAcquisitionTimeout">
                        <xsl:value-of select="locking/@lockAcquisitionTimeout"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="locking[@writeSkewCheck]">
                     <xsl:attribute name="writeSkewCheck">
                        <xsl:value-of select="locking/@writeSkewCheck"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="locking[@concurrencyLevel]">
                     <xsl:attribute name="concurrencyLevel">
                        <xsl:value-of select="locking/@concurrencyLevel"/>
                     </xsl:attribute>
                  </xsl:if>
               </xsl:element>
            </xsl:if>


            <xsl:if test="transaction">
               <xsl:element name="transaction">
                  <xsl:if test="transaction[@transactionManagerLookupClass]">
                     <xsl:if
                           test="not(starts-with(transaction/@transactionManagerLookupClass,'org.jboss.cache'))">
                        <xsl:message terminate="no">WARNING! Custom 'transactionManagerLookupClass' is being used. This cannot be automatically transformed.</xsl:message>
                     </xsl:if>
                     <xsl:attribute name="transactionManagerLookupClass">
                        <xsl:value-of
                              select="concat('org.infinispan.transaction.lookup', substring-after(transaction/@transactionManagerLookupClass,'org.jboss.cache.transaction'))"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="transaction[@syncRollbackPhase]">
                     <xsl:attribute name="syncRollbackPhase">
                        <xsl:value-of select="transaction/@syncRollbackPhase"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="transaction[@syncCommitPhase]">
                     <xsl:attribute name="syncCommitPhase">
                        <xsl:value-of select="transaction/@syncCommitPhase"/>
                     </xsl:attribute>
                  </xsl:if>
               </xsl:element>
            </xsl:if>

            <xsl:if test="jmxStatistics[@enabled]">
               <xsl:element name="jmxStatistics">
                  <xsl:attribute name="enabled">
                     <xsl:value-of select="jmxStatistics/@enabled"/>
                  </xsl:attribute>
               </xsl:element>
            </xsl:if>

            <xsl:if test="serialization[@useLazyDeserialization]">
               <xsl:element name="storeAsBinary">
                  <xsl:attribute name="enabled">
                     <xsl:value-of select="serialization/@useLazyDeserialization"/>
                  </xsl:attribute>
               </xsl:element>
            </xsl:if>

            <xsl:if test="invocationBatching[@enabled]">
               <xsl:element name="invocationBatching">
                  <xsl:attribute name="enabled">
                     <xsl:value-of select="invocationBatching/@enabled"/>
                  </xsl:attribute>
               </xsl:element>
            </xsl:if>

            <xsl:if test="clustering">
               <xsl:element name="clustering">
                  <xsl:if test="clustering[@mode]">
                     <xsl:attribute name="mode">
                        <xsl:value-of select="clustering/@mode"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="clustering/stateRetrieval">
                     <xsl:element name="stateRetrieval">
                        <xsl:if test="clustering/stateRetrieval[@timeout]">
                           <xsl:attribute name="timeout">
                              <xsl:value-of select="clustering/stateRetrieval/@timeout"/>
                           </xsl:attribute>
                        </xsl:if>
                        <xsl:if test="clustering/stateRetrieval[@fetchInMemoryState]">
                           <xsl:attribute name="fetchInMemoryState">
                              <xsl:value-of select="clustering/stateRetrieval/@fetchInMemoryState"/>
                           </xsl:attribute>
                        </xsl:if>
                     </xsl:element>
                  </xsl:if>
                  <xsl:if test="clustering/sync">
                     <xsl:element name="sync">
                        <xsl:if test="clustering/sync[@replTimeout]">
                           <xsl:attribute name="replTimeout">
                              <xsl:value-of select="clustering/sync/@replTimeout"/>
                           </xsl:attribute>
                        </xsl:if>
                     </xsl:element>
                  </xsl:if>
                  <xsl:if test="clustering/async">
                     <xsl:element name="async">
                        <xsl:if test="clustering/async[@useReplQueue]">
                           <xsl:attribute name="useReplQueue">
                              <xsl:value-of select="clustering/async/@useReplQueue"/>
                           </xsl:attribute>
                        </xsl:if>
                        <xsl:if test="clustering/async[@replQueueInterval]">
                           <xsl:attribute name="replQueueInterval">
                              <xsl:value-of select="clustering/async/@replQueueInterval"/>
                           </xsl:attribute>
                        </xsl:if>
                        <xsl:if test="clustering/async[@replQueueMaxElements]">
                           <xsl:attribute name="replQueueMaxElements">
                              <xsl:value-of select="clustering/async/@replQueueMaxElements"/>
                           </xsl:attribute>
                        </xsl:if>
                        <xsl:if test="clustering/async[@serializationExecutorPoolSize > 1]">
                           <xsl:attribute name="asyncMarshalling">true</xsl:attribute>
                        </xsl:if>
                     </xsl:element>
                  </xsl:if>
               </xsl:element>
            </xsl:if>

            <xsl:call-template name="generateLoaders"/>
         </default>

         <xsl:for-each select="eviction/region">
            <xsl:element name="namedCache">
               <xsl:call-template name="evictionAttributes"/>
               <xsl:call-template name="generateLoaders"/>
            </xsl:element>
         </xsl:for-each>
      </xsl:element>

      <xsl:message terminate="no">IMPORTANT: Please take a look at the generated file for (possible) TODOs about the elements that couldn't be converted automatically!</xsl:message>
   </xsl:template>

   <xsl:template name="generateLoaders">
      <xsl:if test="loaders">
         <xsl:element name="loaders">
            <xsl:if test="loaders[@passivation]">
               <xsl:attribute name="passivation">
                  <xsl:value-of select="loaders/@passivation"/>
               </xsl:attribute>
            </xsl:if>
            <xsl:if test="loaders[@shared]">
               <xsl:attribute name="shared">
                  <xsl:value-of select="loaders/@shared"/>
               </xsl:attribute>
            </xsl:if>
            <xsl:if test="loaders/preload">
               <xsl:message terminate="no">WARNING! Preload elements cannot be automatically transformed, please do it manually!</xsl:message>
               <!-- TODO - Preload elements cannot be automatically transformed, please do it manually!-->
            </xsl:if>
            <xsl:for-each select="loaders/loader">
               <xsl:element name="loader">
                  <xsl:attribute name="class">
                     <xsl:choose>
                        <xsl:when test="@class='org.jboss.cache.loader.JDBCCacheLoader'">
                           <xsl:text>org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore</xsl:text>
                        </xsl:when>
                        <xsl:otherwise>
                           <xsl:choose>
                              <xsl:when test="@class='org.jboss.cache.loader.FileCacheLoader'">
                                 <xsl:text>org.infinispan.loaders.file.FileCacheStore</xsl:text>
                              </xsl:when>
                              <xsl:otherwise>
                                 <xsl:choose>
                                    <xsl:when test="@class='org.jboss.cache.loader.bdbje.BdbjeCacheLoader'">
                                       <xsl:text>org.infinispan.loaders.bdbje.BdbjeCacheStore</xsl:text>
                                    </xsl:when>
                                    <xsl:otherwise>
                                       <xsl:choose>
                                          <xsl:when
                                                test="@class='org.jboss.cache.loader.jdbm.JdbmCacheLoader' or @class='org.jboss.cache.loader.jdbm.JdbmCacheLoader2'">
                                             <xsl:text>org.infinispan.loaders.jdbm.JdbmCacheStore</xsl:text>
                                          </xsl:when>
                                          <xsl:otherwise>
                                             <xsl:choose>
                                                <xsl:when test="@class='org.jboss.cache.loader.s3.S3CacheLoader'">
                                                   <xsl:text>org.infinispan.loaders.cloud.CloudCacheStore</xsl:text>
                                                </xsl:when>
                                                <xsl:otherwise>
                                                   <xsl:message terminate="no">WARNING! Cannot convert classloader's class, please do it manually!</xsl:message>
                                                   <!--TODO Cannot convert classloader's class, please do it manually!-->
                                                </xsl:otherwise>
                                             </xsl:choose>
                                          </xsl:otherwise>
                                       </xsl:choose>
                                    </xsl:otherwise>
                                 </xsl:choose>
                              </xsl:otherwise>
                           </xsl:choose>
                        </xsl:otherwise>
                     </xsl:choose>
                  </xsl:attribute>
                  <xsl:if test="@fetchPersistentState">
                     <xsl:attribute name="fetchPersistentState">
                        <xsl:value-of select="@fetchPersistentState"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="@ignoreModifications">
                     <xsl:attribute name="ignoreModifications">
                        <xsl:value-of select="@ignoreModifications"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="@purgeOnStartup">
                     <xsl:attribute name="purgeOnStartup">
                        <xsl:value-of select="@purgeOnStartup"/>
                     </xsl:attribute>
                  </xsl:if>
                  <xsl:if test="singletonStore">
                     <xsl:element name="singletonStore">
                        <xsl:if test="singletonStore[@enabled]">
                           <xsl:attribute name="enabled">
                              <xsl:value-of select="singletonStore/@enabled"/>
                           </xsl:attribute>
                           <xsl:if test="singletonStore/properties">
                              <xsl:message terminate="no">WARNING! Singleton store was changed and needs to be configured manually!</xsl:message>
                              <!-- TODO Singleton store was changed and needs to be configured manually-->
                           </xsl:if>
                        </xsl:if>
                     </xsl:element>
                  </xsl:if>
                  <xsl:if test="@async">
                     <async enabled="true"/>
                  </xsl:if>
                  <xsl:if test="properties">
                     <xsl:message terminate="no">WARNING! Please configure cache loader props manually!</xsl:message>
                     <!--<properties>-->
                        <!--<property name="TODO set name here..." value="...set value here..."/>-->
                        <!--<property name="TODO set name here..." value="...set value here..."/>-->
                     <!--</properties>-->
                  </xsl:if>
               </xsl:element>
            </xsl:for-each>
         </xsl:element>
         <xsl:if test="/jbosscache/eviction/default">
            <xsl:for-each select="/jbosscache/eviction/default">
               <xsl:call-template name="evictionAttributes"/>
            </xsl:for-each>
         </xsl:if>
      </xsl:if>
   </xsl:template>

   <xsl:template name="evictionAttributes">

      <xsl:if test="@name">
         <xsl:attribute name="name">
            <xsl:value-of select="@name"/>
         </xsl:attribute>
      </xsl:if>
      <xsl:element name="eviction">
         <xsl:if test="/jbosscache/eviction[@wakeUpInterval]">
            <xsl:attribute name="wakeUpInterval">
               <xsl:value-of select="/jbosscache/eviction/@wakeUpInterval"/>
            </xsl:attribute>
         </xsl:if>
         <xsl:if test="property[@name='maxNodes']">
            <xsl:attribute name="maxEntries">
               <xsl:value-of select="normalize-space(property[@name='maxNodes']/@value)"/>
            </xsl:attribute>
         </xsl:if>
         <xsl:if test="@algorithmClass and not(starts-with(@algorithmClass,'org.jboss.cache'))">
            <xsl:message terminate="no">WARNING! Custom eviction 'algorithmClass' is being used. This cannot be automatically transformed. Plese do this manually. </xsl:message>
            <!--TODO Custom eviction 'algorithmClass' is being used. This cannot be automatically transformed. Plese do this manually.-->
         </xsl:if>
         <xsl:choose>
            <xsl:when test="@algorithmClass">
               <xsl:attribute name="strategy">
                  <xsl:value-of
                        select="substring-before(substring-after(@algorithmClass,'org.jboss.cache.eviction.'),'Algorithm')"/>
               </xsl:attribute>
            </xsl:when>
            <xsl:otherwise>
               <xsl:attribute name="strategy">
                  <xsl:value-of
                        select="substring-before(substring-after(/jbosscache/eviction/default/@algorithmClass,'org.jboss.cache.eviction.'),'Algorithm')"/>
               </xsl:attribute>
            </xsl:otherwise>
         </xsl:choose>
      </xsl:element>
      <xsl:if test="property[@name='timeToLive'] or property[@name='maxAge']">
         <xsl:element name="expiration">
            <xsl:if test="property[@name='timeToLive']">
               <xsl:attribute name="maxIdle">
                  <xsl:value-of select="property[@name='timeToLive']/@value"/>
               </xsl:attribute>
            </xsl:if>
            <xsl:if test="property[@name='maxAge']">
               <xsl:attribute name="lifespan">
                  <xsl:value-of select="property[@name='maxAge']/@value"/>
               </xsl:attribute>
            </xsl:if>
         </xsl:element>
      </xsl:if>
   </xsl:template>
</xsl:stylesheet>
