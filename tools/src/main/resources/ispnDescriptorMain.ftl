<#--
/*
 * RHQ Management Platform
 * Copyright (C) 2005-2011 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */
-->
<#-- @ftlvariable name="props" type="org.rhq.helpers.pluginGen.Props" -->
<#--
    This file contains the body of the descriptor for a single
    platform/server/service. It is called by descriptor.ftl

-->
name="${props.name}"
            discovery="${props.discoveryClass}"
            class="${props.componentClass}"
            <#if props.singleton>singleton="true"</#if>
            <#if props.manualAddOfResourceType>supportsManualAdd="true"</#if>
            <#if props.createChildren && props.deleteChildren>createDeletePolicy="both"<#elseif props.createChildren && !props.deleteChildren>createDeletePolicy="create-only"<#elseif !props.createChildren && props.deleteChildren>createDeletePolicy="delete-only"<#else > <#-- Dont mention it, as 'neither' is default --></#if>
          >

          <#if props.runsInsides?has_content>
            <runs-inside>
              <#list props.runsInsides as typeKey>
                <parent-resource-type name="${typeKey.name}" plugin="${typeKey.pluginName}"/>
              </#list>
            </runs-inside>
          </#if>

          <#if props.simpleProps?has_content>
            <plugin-configuration>
                <#list props.simpleProps as simpleProps>
                <c:simple-property name="${simpleProps.name}" <#if simpleProps.description??>description="${simpleProps.description}"</#if> <#if simpleProps.type??>type="${simpleProps.type}"</#if> <#if simpleProps.defaultValue??>default="${simpleProps.defaultValue}"</#if> <#if simpleProps.readOnly>readOnly="true"</#if>/>
                </#list>
                <!-- The template section is only for manual resource additions, and default parameters and the ones presented to the user. -->
                <#list props.templates as templates>
                <c:template name="${templates.name}" description="${templates.description}">
                  <#list templates.simpleProps as innerSimpleProps>
                  <c:simple-property name="${innerSimpleProps.name}" displayName="${innerSimpleProps.displayName}"
                                     defaultValue="${innerSimpleProps.defaultValue}"/>
                  </#list>
                </c:template>
                </#list>
            </plugin-configuration>
         </#if>

     <#if props.hasOperations>
        <#if props.operations?has_content>
           <#list props.operations as operation>
           <operation name="${operation.name}" displayName="${operation.displayName}" description="${operation.description}">
              <#if operation.params?has_content>
              <parameters>
              <#list operation.params as param>
                 <c:simple-property name="${param.name}" <#if param.description??>description="${param.description}"</#if>/>
              </#list>
              </parameters>
              </#if>
              <#if operation.result??>
              <results>
                 <c:simple-property name="${operation.result.name}" />
              </results>
              </#if>
           </operation>
           </#list>
        <#else>
           <operation name="dummyOperation">
              <!-- TODO supply parameters and return values -->
           </operation>
        </#if>
     </#if>

     <#if props.hasMetrics>
        <#if props.metrics?has_content>
           <#list props.metrics as metric>
           <metric property="${metric.property}" displayName="${metric.displayName}" displayType="${metric.displayType}" units="${metric.units}" dataType="${metric.dataType}"
                   description="${metric.description}" />
           </#list>
        <#else>
           <metric property="dummyMetric" displayName="Dummy display name"/>
        </#if>
     </#if>

        <#if props.events>
            <event name="${props.name}DummyEvent"/>
        </#if>
        <#if props.resourceConfiguration>
            <resource-configuration>
                <!-- TODO supply your configuration parameters -->
                <c:simple-property name="dummy"/>
            </resource-configuration>
        </#if>