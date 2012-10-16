<#--
/*
 * RHQ Management Platform
 * Copyright (C) 2005-2008 Red Hat, Inc.
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
<?xml version="1.0"?>
<plugin name="${props.pluginName}"
        displayName="${props.pluginName}Plugin"
        description="${props.pluginDescription}"
<#if props.usePluginLifecycleListenerApi>
        pluginLifecycleListener="${props.componentClass}"
</#if>
        package="${props.pkg}"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="urn:xmlns:rhq-plugin"
        xmlns:c="urn:xmlns:rhq-configuration">

<#if props.dependsOnJmxPlugin>
   <depends plugin="JMX" useClasses="true"/>
</#if>

   <${props.category.lowerName} <#include "ispnDescriptorMain.ftl"/>

   <#-- Those are the embedded children -->
   <#list props.children as props>
       <${props.category.lowerName} <#include "./ispnDescriptorMain.ftl"/>
       </${props.category.lowerName}>
   </#list>
   </${props.category.lowerName}>

</plugin>
