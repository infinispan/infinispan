<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet xmlns="urn:infinispan:config:4.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
   <xsl:output method="xml" indent="yes" version="1.0" encoding="UTF-8" omit-xml-declaration="no"/>
   <xsl:template match="/ehcache">
      <xsl:comment>
         This XSL stylesheet is used to convert an Ehcache 1.6.x configuration into an Infinispan 4.0.x configuration.
         Note that Infinispan supports JGroups only, caches are migrated to using JGroups.
         Peer discovery will also be using JGroups. Eviction policies are translated to LRU, FIFO or NONE.
      </xsl:comment>
      <xsl:element name="infinispan">

         <xsl:element name="global">
            <xsl:element name="asyncListenerExecutor">
               <xsl:attribute name="factory">org.infinispan.executors.DefaultExecutorFactory</xsl:attribute>
               <property name="threadNamePrefix" value="AsyncListenerThread"/>
            </xsl:element>

            <xsl:element name="asyncSerializationExecutor">
               <xsl:attribute name="factory">org.infinispan.executors.DefaultExecutorFactory</xsl:attribute>
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
               <xsl:attribute name="enabled">
                  <xsl:text>true</xsl:text>
               </xsl:attribute>
            </xsl:element>

            <xsl:element name="shutdown">
               <xsl:attribute name="hookBehavior">
                  <xsl:text>DEFAULT</xsl:text>
               </xsl:attribute>
            </xsl:element>
         </xsl:element>

         <xsl:element name="default">
            <xsl:if test="defaultCache[@memoryStoreEvictionPolicy]">
               <xsl:if test="contains(defaultCache[@memoryStoreEvictionPolicy], 'LRU')">
                  <xsl:element name="eviction">
                     <xsl:attribute name="strategy">
                        <xsl:value-of select="defaultCache/@memoryStoreEvictionPolicy"/>
                     </xsl:attribute>
                  </xsl:element>
               </xsl:if>
               <xsl:if test="contains(defaultCache[@memoryStoreEvictionPolicy], 'FIFO')">
                  <xsl:element name="eviction">
                     <xsl:attribute name="strategy">
                        <xsl:value-of select="defaultCache/@memoryStoreEvictionPolicy"/>
                     </xsl:attribute>
                  </xsl:element>
               </xsl:if>
               <xsl:if test="contains(defaultCache[@memoryStoreEvictionPolicy], 'LFU')">
                  <xsl:message terminate="no">WARNING!!! Infinispan does not support LFU eviction. Using LRU instead.
                  </xsl:message>
                  <xsl:element name="eviction">
                     <xsl:attribute name="strategy">
                        <xsl:text>LRU</xsl:text>
                     </xsl:attribute>
                  </xsl:element>
               </xsl:if>
            </xsl:if>

         </xsl:element>
         <xsl:for-each select="cache">
            <xsl:element name="namedCache">
               <xsl:attribute name="name">
                  <xsl:value-of select="@name"/>
               </xsl:attribute>
               <xsl:element name="eviction">
                  <xsl:attribute name="strategy">
                     <xsl:choose>
                        <xsl:when test="contains(@memoryStoreEvictionPolicy, 'LRU')">
                           <xsl:text>LRU</xsl:text>
                        </xsl:when>
                        <xsl:otherwise>
                           <xsl:choose>
                              <xsl:when test="contains(@memoryStoreEvictionPolicy, 'FIFO')">
                                 <xsl:text>FIFO</xsl:text>
                              </xsl:when>
                              <xsl:otherwise>
                                 <xsl:choose>
                                    <xsl:when test="contains(@memoryStoreEvictionPolicy, 'LFU')">
                                       <xsl:text>LRU</xsl:text>
                                    </xsl:when>
                                    <xsl:otherwise>
                                       <xsl:text>NONE</xsl:text>
                                    </xsl:otherwise>
                                 </xsl:choose>
                              </xsl:otherwise>
                           </xsl:choose>
                        </xsl:otherwise>
                     </xsl:choose>
                  </xsl:attribute>
               </xsl:element>
               <!-- <expiration lifespan="60000" maxIdle="1000" /> -->
               <xsl:element name="expiration">
                  
               </xsl:element>

               <xsl:if test="cacheEventListenerFactory">
                  <xsl:element name="clustering">
                     <xsl:if test="cacheEventListenerFactory/@properties">
                        <xsl:attribute name="mode">
                           <!-- Replication, Invalidation, Distribution-->
                           <xsl:text>distribution</xsl:text>
                        </xsl:attribute>
                        <!-- TODO remove spaces -->
                        <xsl:choose>
                           <xsl:when test="contains(cacheEventListenerFactory/@properties, 'replicateAsynchronously=false')">
                              <xsl:element name="sync">
                                 <!-- replQueueInterval="100" replQueueMaxElements="200" ?? -->
                                 <xsl:attribute name="useReplQueue">
                                    <xsl:text>true</xsl:text>
                                 </xsl:attribute>
                              </xsl:element>
                           </xsl:when>
                           <xsl:otherwise>
                              <xsl:element name="async">
                                 <!-- replQueueInterval="100" replQueueMaxElements="200" ?? -->
                                 <xsl:attribute name="useReplQueue">
                                    <xsl:text>true</xsl:text>
                                 </xsl:attribute>
                              </xsl:element>
                           </xsl:otherwise>
                        </xsl:choose>
                     </xsl:if>
                  </xsl:element>
               </xsl:if>
            </xsl:element>
         </xsl:for-each>
      </xsl:element>


   </xsl:template>
</xsl:stylesheet>
