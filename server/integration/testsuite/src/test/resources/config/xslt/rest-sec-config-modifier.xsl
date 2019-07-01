<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="xml" indent="yes"/>

    <xsl:variable name="nsS">urn:jboss:domain:security:</xsl:variable>
    <xsl:variable name="nsE">urn:infinispan:server:endpoint:</xsl:variable>
    <xsl:variable name="nsW">urn:jboss:domain:web:</xsl:variable>

    <xsl:param name="security.realm" select="'ApplicationRealm'"/>
    <xsl:param name="auth.method" select="'BASIC'"/>
    <xsl:param name="cache.container" select="'${connector.cache.container}'"/>
    <xsl:param name="modifyCertSecRealm" select="false"/>

    <!-- New rest-connector definition -->
    <xsl:variable name="newRESTEndpointDefinition">
        <xsl:choose>
            <xsl:when test="$auth.method != 'CLIENT_CERT'">
                <rest-connector socket-binding="rest">
                    <xsl:attribute name="cache-container">
                        <xsl:value-of select="$cache.container"/>
                    </xsl:attribute>
                    <authentication>
                        <xsl:attribute name="security-realm">
                            <xsl:value-of select="$security.realm"/>
                        </xsl:attribute>
                        <xsl:attribute name="auth-method">
                            <xsl:value-of select="$auth.method"/>
                        </xsl:attribute>
                    </authentication>
                </rest-connector>
            </xsl:when>
            <xsl:otherwise>
                <rest-connector socket-binding="rest">
                    <xsl:attribute name="cache-container">
                        <xsl:value-of select="$cache.container"/>
                    </xsl:attribute>
                </rest-connector>
                <rest-connector socket-binding="rest-ssl" name="rest-ssl">
                    <xsl:attribute name="cache-container">
                        <xsl:value-of select="$cache.container"/>
                    </xsl:attribute>
                    <authentication>
                        <xsl:attribute name="security-realm">
                            <xsl:value-of select="$security.realm"/>
                        </xsl:attribute>
                        <xsl:attribute name="auth-method">
                            <xsl:value-of select="$auth.method"/>
                        </xsl:attribute>
                    </authentication>
                    <encryption require-ssl-client-auth="true">
                        <xsl:attribute name="security-realm">
                            <xsl:value-of select="$security.realm"/>
                        </xsl:attribute>
                    </encryption>
                </rest-connector>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>

    <!-- Replace rest-connector element with new one - secured -->
    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsE)]/*[local-name()='rest-connector']">
        <xsl:copy-of select="$newRESTEndpointDefinition"/>
    </xsl:template>

    <xsl:template
            match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsE)]/*/@cache-container">
        <xsl:attribute name="cache-container">
            <xsl:value-of select="$cache.container"/>
        </xsl:attribute>
    </xsl:template>

    <!-- Copy everything else. -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>

