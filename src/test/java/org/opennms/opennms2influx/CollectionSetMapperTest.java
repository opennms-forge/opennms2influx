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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Point;
import org.junit.Test;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.CollectionSet;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.CollectionSetResource;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.NodeLevelResource;
import org.opennms.features.kafka.collection.persistence.CollectionSetProtos.NumericAttribute;

public class CollectionSetMapperTest {

    CollectionSetMapper mapper = new CollectionSetMapper();

    @Test
    public void canMapCollectionSet() {
        // A simple empty collection set
        CollectionSet collectionSet = CollectionSet.newBuilder()
                .build();
        List<Point> points = mapper.toPoints(collectionSet);
        assertThat(points, hasSize(0));

        collectionSet = CollectionSet.newBuilder()
                .setTimestamp(1L)
                .addResource(CollectionSetResource.newBuilder()
                    .setNode(NodeLevelResource.newBuilder().setNodeId(1))
                    .addNumeric(NumericAttribute.newBuilder()
                            .setGroup("group")
                            .setName("metric")
                            .setValue(0.9d)
                            .setType(NumericAttribute.Type.GAUGE)
                    )
                )
                .build();

        points = mapper.toPoints(collectionSet);
        assertThat(points, hasSize(1));
        assertThat(points, contains(Point.measurement("group")
                .time(1, TimeUnit.MILLISECONDS)
                .tag("nodeId", "1")
                .addField("metric", 0.9d)
                .build()));
    }
}
