/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RetryFailedAllocationIT extends ESIntegTestCase {

    public void testRerouteAfterMaxRetries() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)).get();
        ensureGreen();

        logger.info("--> disabling allocation to capture shard failure");
        disableAllocation("test");

        AllocationCommand ac = new AllocateReplicaAllocationCommand("test", 1, "testes");
        ClusterRerouteRequestBuilder rerouteRequestBuilder = client().admin().cluster().prepareReroute()
            .setRetryFailed(true)
            .add(ac);
    }

    /**
     * Verifies that when there is no delay timeout, a 1/1 index shard will immediately
     * get allocated to a free node when the node hosting it leaves the cluster.
     */
    public void testNoDelayedTimeout() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0)).get();
        ensureGreen("test");
        indexRandomData();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(findNodeWithShard()));
        assertThat(client().admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(0));
        ensureGreen("test");
    }

    /**
     * When we do have delayed allocation set, verifies that even though there is a node
     * free to allocate the unassigned shard when the node hosting it leaves, it doesn't
     * get allocated. Once we bring the node back, it gets allocated since it existed
     * on it before.
     */
    public void testDelayedAllocationNodeLeavesAndComesBack() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1))).get();
        ensureGreen("test");
        indexRandomData();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(findNodeWithShard()));
        assertBusy(() -> assertThat(client().admin().cluster().prepareState().all().get().getState()
            .getRoutingNodes().unassigned().size() > 0, equalTo(true)));
        assertThat(client().admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(1));
        internalCluster().startNode(); // this will use the same data location as the stopped node
        ensureGreen("test");
    }
}
