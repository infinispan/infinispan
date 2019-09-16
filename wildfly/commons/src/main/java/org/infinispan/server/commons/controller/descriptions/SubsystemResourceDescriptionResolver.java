/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2015, Red Hat, Inc., and individual contributors
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

package org.infinispan.server.commons.controller.descriptions;

import java.util.List;

import org.jboss.as.controller.Extension;
import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;

/**
 * Consolidates common logic when creating {@link org.jboss.as.controller.descriptions.ResourceDescriptionResolver}s for a given subsystem.
 * @author Paul Ferraro
 */
public class SubsystemResourceDescriptionResolver extends StandardResourceDescriptionResolver {

    private static final String RESOURCE_NAME_PATTERN = "%s.LocalDescriptions";

    protected SubsystemResourceDescriptionResolver(String subsystemName, List<String> keyPrefixes, Class<? extends Extension> extensionClass) {
        super(join(subsystemName, keyPrefixes), String.format(RESOURCE_NAME_PATTERN, extensionClass.getPackage().getName()), extensionClass.getClassLoader(), true, false);
    }

    private static String join(String subsystemName, List<String> keyPrefixes) {
        if (keyPrefixes.isEmpty()) return subsystemName;
        StringBuilder result = new StringBuilder(subsystemName);
        for (String keyPrefix : keyPrefixes) {
            result.append('.').append(keyPrefix);
        }
        return result.toString();
    }
}
