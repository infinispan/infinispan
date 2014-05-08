<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="xml" indent="yes"/>

    <xsl:variable name="nsS">urn:jboss:domain:security:</xsl:variable>
    <xsl:variable name="nsE">urn:infinispan:server:endpoint:</xsl:variable>
    <xsl:variable name="nsW">urn:jboss:domain:web:</xsl:variable>

    <xsl:param name="security.domain" select="'other'"/>
    <xsl:param name="security.mode" select="'WRITE'"/>
    <xsl:param name="auth.method" select="'BASIC'"/>
    <xsl:param name="cache.container" select="'${connector.cache.container}'"/>
    <xsl:param name="modifyCertSecDomain" select="false"/>
    <xsl:param name="modifyDigestSecDomain" select="false"/>

    <!-- New rest-connector definition -->
    <xsl:variable name="newRESTEndpointDefinition">
        <rest-connector virtual-server="default-host">
            <xsl:attribute name="cache-container">
                <xsl:value-of select="$cache.container"/>
            </xsl:attribute>
            <xsl:attribute name="security-domain">
                <xsl:value-of select="$security.domain"/>
            </xsl:attribute>
            <xsl:attribute name="auth-method">
                <xsl:value-of select="$auth.method"/>
            </xsl:attribute>
            <xsl:attribute name="security-mode">
                <xsl:value-of select="$security.mode"/>
            </xsl:attribute>
        </rest-connector>
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

    <!-- New CERT security-domain definition -->
    <xsl:variable name="newClientCertSecurityDomainDefinition">
        <security-domain name="client_cert_auth" cache-type="infinispan">
            <authentication>
                <login-module code="CertificateRoles" flag="required">
                    <module-option name="securityDomain" value="client_cert_auth"/>
                    <module-option name="rolesProperties" value="${{jboss.server.config.dir}}/roles.properties"/>
                </login-module>
            </authentication>
            <jsse truststore-password="changeit" client-auth="true" truststore-url="${{jboss.server.config.dir}}/jsse.keystore"/>
        </security-domain>
    </xsl:variable>

    <!-- New DIGEST security-domain definition -->
    <xsl:variable name="newDigestSecurityDomainDefinition">
        <security-domain name="digest_auth" cache-type="infinispan">
            <authentication>
                <login-module code="UsersRoles" flag="required">
                    <module-option name="hashAlgorithm" value="MD5"/>
                    <module-option name="hashEncoding" value="rfc2617"/>
                    <module-option name="hashUserPassword" value="false"/>
                    <module-option name="hashStorePassword" value="true"/>
                    <module-option name="passwordIsA1Hash" value="true"/>
                    <module-option name="storeDigestCallback" value="org.jboss.security.auth.callback.RFC2617Digest"/>
                    <module-option name="usersProperties" value="${{jboss.server.config.dir}}/application-users.properties"/>
                    <module-option name="rolesProperties" value="${{jboss.server.config.dir}}/application-roles.properties"/>
                </login-module>
            </authentication>
        </security-domain>
    </xsl:variable>

    <!-- Add another security domain -->
    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsS)]
        /*[local-name()='security-domains' and starts-with(namespace-uri(), $nsS)]">
        <xsl:copy>
            <xsl:if test="$modifyCertSecDomain != 'false'">
                <xsl:copy-of select="$newClientCertSecurityDomainDefinition"/>
            </xsl:if>
            <xsl:if test="$modifyDigestSecDomain != 'false'">
                <xsl:copy-of select="$newDigestSecurityDomainDefinition"/>
            </xsl:if>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

    <!-- New connector definition -->
    <xsl:variable name="newHttpsConnector">
        <connector name="https" protocol="HTTP/1.1" scheme="https" socket-binding="https" enable-lookups="false" secure="true">
            <ssl name="myssl"
                 keystore-type="JKS"
                 password="changeit"
                 key-alias="test"
                 truststore-type="JKS"
                 verify-client="want"
                 certificate-key-file="${{jboss.server.config.dir}}/server.keystore"
                 ca-certificate-file="${{jboss.server.config.dir}}/server.keystore"
                    />
        </connector>
    </xsl:variable>

    <!-- Add another connector -->
    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsW)]/*[local-name()='connector'][position()=last()]">
        <xsl:if test="$modifyCertSecDomain != 'false'">
            <xsl:copy>
                <xsl:apply-templates select="@*|node()"/>
            </xsl:copy>
            <xsl:copy-of select="$newHttpsConnector"/>
        </xsl:if>
    </xsl:template>

    <!-- Copy everything else. -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>

