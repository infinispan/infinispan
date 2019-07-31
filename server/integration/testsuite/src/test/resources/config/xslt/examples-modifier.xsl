<xsl:stylesheet version="2.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:p="urn:jboss:domain:8.0"
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
    <xsl:param name="trace">none</xsl:param>

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

    <!--
        An XSLT style sheet which will enable trace logging for the test suite.
        This can be enabled via -Dtrace=org.infinispan.category1,org.jgroups.category2
    -->
    <xsl:template name="output-loggers">
        <xsl:param name="list" />
        <xsl:variable name="first" select="substring-before(concat($list,','), ',')" />
        <xsl:variable name="remaining" select="substring-after($list, ',')"/>
        <xsl:element name="logger">
            <xsl:attribute name="category">
                <xsl:value-of select="$first"></xsl:value-of>
            </xsl:attribute>
            <xsl:element name="level">
                <xsl:attribute name="name">
                    <xsl:value-of select="'TRACE'"/>
                </xsl:attribute>
            </xsl:element>
        </xsl:element>
        <xsl:if test="string-length($remaining) > 0">
            <xsl:call-template name="output-loggers">
                <xsl:with-param name="list" select="$remaining" />
            </xsl:call-template>
        </xsl:if>
    </xsl:template>

    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsLogging)]/*[local-name()='console-handler']" use-when="$trace">
        <xsl:choose>
            <xsl:when test="$trace='none'">
                <xsl:copy>
                    <xsl:apply-templates select="node()|@*"/>
                </xsl:copy>
            </xsl:when>
            <xsl:otherwise>
                <xsl:copy>
                    <xsl:attribute name="name">
                        <xsl:value-of select="'CONSOLE'"/>
                    </xsl:attribute>
                    <!--define INFO log level for console logger, otherwise it slows down running tests with multiple servers-->
                    <level name="INFO"/>
                    <formatter>
                        <named-formatter name="COLOR-PATTERN"/>
                    </formatter>
                </xsl:copy>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsLogging)]/*[local-name()='periodic-rotating-file-handler']" use-when="$trace">
        <xsl:choose>
            <xsl:when test="$trace='none'">
                <xsl:copy>
                    <xsl:apply-templates select="node()|@*"/>
                </xsl:copy>
            </xsl:when>
            <xsl:otherwise>
                <file-handler>
                    <xsl:attribute name="name">
                        <xsl:value-of select="'FILE'"/>
                    </xsl:attribute>
                    <level name="TRACE"/>
                    <formatter>
                        <named-formatter name="PATTERN"/>
                    </formatter>
                    <file relative-to="jboss.server.log.dir" path="server.log"/>
                    <append value="true"/>
                </file-handler>
                <xsl:call-template name="output-loggers">
                    <xsl:with-param name="list" select="$trace" />
                </xsl:call-template>
            </xsl:otherwise>
        </xsl:choose>
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
