/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2018 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2018 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.opennms2influx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.influxdb.dto.Point;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.CollectionSetResource;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.GenericTypeResource;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.InterfaceLevelResource;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.NodeLevelResource;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.NumericAttribute;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.StringAttribute;

public class CollectionSetMapper {

    public List<Point> toPoints(CollectionSetProtos.CollectionSet collectionSet) {
        final List<Point> points = new ArrayList<>();
        final long timestamp = collectionSet.getTimestamp();
        for (CollectionSetResource resource : collectionSet.getResourceList()) {
            final Map<String,String> resourceTags = new HashMap<>();
            final NodeLevelResource nodeLevelResource;
            if (resource.hasInterface()) {
                final InterfaceLevelResource interfaceLevelResource = resource.getInterface();
                nodeLevelResource = interfaceLevelResource.getNode();
                resourceTags.put("interface", interfaceLevelResource.getInstance());
            } else if (resource.hasGeneric()) {
                final GenericTypeResource genericTypeResource = resource.getGeneric();
                nodeLevelResource = genericTypeResource.getNode();
            } else {
                nodeLevelResource = resource.getNode();
            }

            resourceTags.put("nodeId", Long.toString(nodeLevelResource.getNodeId()));
            if (nodeLevelResource.getForeignId() != null) {
                resourceTags.put("foreignId", nodeLevelResource.getForeignId());
            }
            if (nodeLevelResource.getForeignSource() != null) {
                resourceTags.put("foreignSource", nodeLevelResource.getForeignSource());
            }
            if (nodeLevelResource.getNodeLabel() != null) {
                resourceTags.put("nodeLabel", nodeLevelResource.getNodeLabel());
            }

            final Map<String,String> stringAttributes = new HashMap<>();
            for (StringAttribute stringAttribute : resource.getStringList()) {
                stringAttributes.put(stringAttribute.getName(), stringAttribute.getValue());
            }

            final Map<String,List<NumericAttribute>> numericAttributesByGroup = resource.getNumericList().stream()
                    .collect(Collectors.groupingBy(NumericAttribute::getGroup));

            for (Map.Entry<String,List<NumericAttribute>> entry : numericAttributesByGroup.entrySet()) {
                final String groupName = entry.getKey();

                final Point.Builder builder = Point.measurement(groupName)
                        .time(timestamp, TimeUnit.MILLISECONDS)
                        .tag(resourceTags);
                for (NumericAttribute numericAttribute : entry.getValue()) {
                    builder.addField(numericAttribute.getName(), numericAttribute.getValue());
                }
                for (StringAttribute stringAttribute : resource.getStringList()) {
                    builder.addField(stringAttribute.getName(), stringAttribute.getValue());
                }
                points.add(builder.build());
            }
        }
        return points;
    }
}
