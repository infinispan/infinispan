<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet xmlns="urn:infinispan:config:4.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
   <xsl:output method="xml" indent="yes" version="1.0" encoding="UTF-8" omit-xml-declaration="no"/>
   <xsl:template match="/jbosscache">
      <xsl:element name="infinispan">

         <xsl:element name="global">
            <xsl:element name="asyncListenerExecutor">
               <xsl:attribute name="factory">org.infinispan.executors.DefaultExecutorFactory</xsl:attribute>
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
            </xsl:element>

            <xsl:element name="asyncSerializationExecutor">
               <xsl:attribute name="factory">org.infinispan.executors.DefaultExecutorFactory</xsl:attribute>
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
            </xsl:element>

            <evictionScheduledExecutor factory="org.infinispan.executors.DefaultScheduledExecutorFactory">
               <property name="threadNamePrefix" value="EvictionThread"/>
            </evictionScheduledExecutor>

            <replicationQueueScheduledExecutor factory="org.infinispan.executors.DefaultScheduledExecutorFactory">
               <property name="threadNamePrefix" value="ReplicationQueueThread"/>
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
               <xsl:if test="clustering/jgroupsConfig[@configFile]">
                  <xsl:element name="property">
                     <xsl:attribute name="name">configurationFile</xsl:attribute>
                     <xsl:attribute name="value">
                        <xsl:value-of select="clustering/jgroupsConfig/@configFile"/>
                     </xsl:attribute>
                  </xsl:element>
               </xsl:if>
            </xsl:element>

            <xsl:element name="serialization">
               <xsl:attribute name="marshallerClass">org.infinispan.marshall.VersionAwareMarshaller</xsl:attribute>
               <xsl:attribute name="version">1.0</xsl:attribute>
               <xsl:if test="serialization[@objectInputStreamPoolSize]">
                  <xsl:attribute name="objectInputStreamPoolSize">
                     <xsl:value-of select="serialization/@objectInputStreamPoolSize"/>
                  </xsl:attribute>
               </xsl:if>
               <xsl:if test="serialization[@objectOutputStreamPoolSize]">
                  <xsl:attribute name="objectOutputStreamPoolSize">
                     <xsl:value-of select="serialization/@objectOutputStreamPoolSize"/>
                  </xsl:attribute>
               </xsl:if>
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
                        <xsl:message terminate="no">WARNING!!! Custom 'transactionManagerLookupClass' is being used.
                           This cannot
                           be automatically transformed.
                        </xsl:message>
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
               <xsl:element name="lazyDeserialization">
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
                  </xsl:element>
               </xsl:element>

               <xsl:if test="clustering/jgroupsConfig/*">
                  <xsl:message terminate="no">WARNING!!! Use 'transport' element under 'global' config to set up
                     transport!!! Existing JGroups config will be ignored!!
                  </xsl:message>
               </xsl:if>
            </xsl:if>

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
                     <xsl:message terminate="no">WARNING!!! Preload elements cannot be automatically transformed, please
                        do it manually!
                     </xsl:message>
                  </xsl:if>
                  <xsl:for-each select="loaders/loader">
                     <xsl:element name="loader">
                        <xsl:attribute name="class">
                           <xsl:value-of select="@class"/>
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
                        <xsl:if test="properties">
                           <xsl:message terminate="no">INFO: Please configure cache loader props manually!</xsl:message>
                           <properties>
                              <property name="...set name here..." value="...set value here..."/>
                              <property name="...set name here..." value="...set value here..."/>
                           </properties>
                        </xsl:if>
                        <xsl:if test="singletonStore">
                           <xsl:element name="singletonStore">
                              <xsl:if test="singletonStore[@enabled]">
                                 <xsl:attribute name="enabled">
                                    <xsl:value-of select="singletonStore/@enabled"/>
                                 </xsl:attribute>
                                 <xsl:if test="singletonStore/properties">
                                    <xsl:message terminate="no">WARNING!!! Singleton store was changed and needs to be configured manually!!!!</xsl:message>
                                 </xsl:if>
                              </xsl:if>
                           </xsl:element>

                        </xsl:if>
                     </xsl:element>
                  </xsl:for-each>
               </xsl:element>
            </xsl:if>
         </default>

         <xsl:for-each select="eviction/*">
            <xsl:element name="namedCache">
               <xsl:attribute name="name">
                  <xsl:choose>
                     <xsl:when test="@name">
                        <xsl:value-of select="@name"/>
                     </xsl:when>
                     <xsl:otherwise>default</xsl:otherwise>
                  </xsl:choose>
               </xsl:attribute>
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
                     <xsl:message terminate="no">WARNING!!! Custom eviction 'algorithmClass' is being used.
                        This cannot be automatically transformed. Plese do this manually.
                     </xsl:message>
                  </xsl:if>
                  <xsl:choose>
                     <xsl:when test="@algorithmClass">
                        <xsl:attribute name="strategy">
                           <xsl:value-of select="substring-before(substring-after(@algorithmClass,'org.jboss.cache.eviction.'),'Algorithm')"/>
                        </xsl:attribute>
                     </xsl:when>
                     <xsl:otherwise>
                        <xsl:attribute name="strategy">
                           <xsl:value-of select="substring-before(substring-after(/jbosscache/eviction/default/@algorithmClass,'org.jboss.cache.eviction.'),'Algorithm')"/>
                        </xsl:attribute>
                     </xsl:otherwise>
                  </xsl:choose>
                  
               </xsl:element>

            </xsl:element>
         </xsl:for-each>
      </xsl:element>
   </xsl:template>
</xsl:stylesheet>
