<xsl:stylesheet version="2.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:p="urn:jboss:domain:5.0"
                exclude-result-prefixes="p"
>

    <!--Variables of namespaces to be changed-->
    <xsl:variable name="nsLogging">urn:jboss:domain:logging:</xsl:variable>
    <xsl:variable name="nsJGroups">urn:infinispan:server:jgroups:</xsl:variable>
    <xsl:variable name="nsCore">urn:infinispan:server:core:</xsl:variable>
    <xsl:variable name="nsThreads">urn:jboss:domain:threads:</xsl:variable>
    <xsl:variable name="nsSecurity">urn:jboss:domain:security:</xsl:variable>
    <xsl:variable name="nsDatasources">urn:jboss:domain:datasources:</xsl:variable>
    <xsl:variable name="nsEndpoint">urn:infinispan:server:endpoint:</xsl:variable>

    <!-- Parameter declarations with defaults set -->
    <xsl:param name="modifyInfinispan">false</xsl:param>
    <xsl:param name="modifyRelay">false</xsl:param>
    <xsl:param name="modifyMulticastAddress">false</xsl:param>
    <xsl:param name="modifyRemoteDestination">false</xsl:param>
    <xsl:param name="infinispanServerEndpoint">false</xsl:param>
    <xsl:param name="removeRestSecurity">true</xsl:param>
    <xsl:param name="restEncrypt">false</xsl:param>
    <xsl:param name="remoteStoreHrVersion">none</xsl:param>

    <xsl:template match="node()|@*" name="copynode">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsCore)]">
        <xsl:if test="$modifyInfinispan = 'false'">
            <xsl:call-template name="copynode"/>
        </xsl:if>
        <xsl:if test="$modifyInfinispan != 'false'">
            <xsl:copy-of select="document($modifyInfinispan)"/>
        </xsl:if>
    </xsl:template>


    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsJGroups)]//*[local-name()='relay']">
        <xsl:if test="$modifyRelay = 'false'">
            <xsl:call-template name="copynode"/>
        </xsl:if>
        <xsl:if test="$modifyRelay != 'false'">
            <xsl:copy-of select="document($modifyRelay)"/>
        </xsl:if>
    </xsl:template>


    <xsl:template match="p:socket-binding[@name='jgroups-udp']">
        <xsl:if test="$modifyMulticastAddress = 'false'">
            <xsl:call-template name="copynode"/>
        </xsl:if>
        <xsl:if test="$modifyMulticastAddress != 'false'">
            <xsl:copy-of select="document($modifyMulticastAddress)"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="p:remote-destination[@host='remote-host']">
        <xsl:if test="$modifyRemoteDestination = 'false'">
            <xsl:call-template name="copynode"/>
        </xsl:if>
        <xsl:if test="$modifyRemoteDestination != 'false'">
            <xsl:copy-of select="document($modifyRemoteDestination)"/>
        </xsl:if>
    </xsl:template>


    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsEndpoint)]">
        <xsl:if test="$infinispanServerEndpoint = 'false'">
            <xsl:call-template name="copynode"/>
        </xsl:if>
        <xsl:if test="$infinispanServerEndpoint != 'false'">
            <xsl:copy-of select="document($infinispanServerEndpoint)"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsEndpoint)]/*[local-name()='rest-connector']">
        <xsl:if test="$removeRestSecurity = 'false' and $restEncrypt = 'false'">
            <xsl:call-template name="copynode"/>
        </xsl:if>
        <xsl:if test="$removeRestSecurity != 'false' and $restEncrypt = 'false'">
            <xsl:copy>
                <xsl:copy-of select="@*[not(name() = 'security-domain' or name() = 'auth-method')]"/>
                <xsl:apply-templates/>
            </xsl:copy>
        </xsl:if>
        <xsl:if test="$removeRestSecurity = 'false' and $restEncrypt != 'false'">
            <xsl:copy>
                <xsl:copy-of select="@*" />
                <xsl:copy-of select="document($restEncrypt)"/>
            </xsl:copy>
        </xsl:if>
        <xsl:if test="$removeRestSecurity != 'false' and $restEncrypt != 'false'">
            <xsl:copy>
                <xsl:copy-of select="@*[not(name() = 'security-domain' or name() = 'auth-method')]"/>
                <xsl:copy-of select="document($restEncrypt)"/>
            </xsl:copy>
        </xsl:if>
    </xsl:template>


    <!-- matches on the remaining tags and recursively applies templates to their children and copies them to the result -->
    <xsl:template match="*">
        <xsl:copy>
            <xsl:copy-of select="@*"/>
            <xsl:apply-templates/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
