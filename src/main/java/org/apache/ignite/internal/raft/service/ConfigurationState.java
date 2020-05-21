/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft.service;

import java.util.Arrays;
import java.util.UUID;

/**
 * TODO pending group for join consensus and reconfiguration.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class ConfigurationState {
    /**
     * Local ID of the replicator. Must be unique across the whole group lifetime.
     */
    private final UUID locId;

    /**
     * Replication group including local ID, if it is in the configuration.
     * Local ID may be not included to the group if there was a reconfiguration and
     * current node was excluded from the replication group.
     */
    private final UUID[] grp;

    /**
     */
    public ConfigurationState(UUID locId, UUID[] grp) {
        this.locId = locId;
        this.grp = Arrays.copyOf(grp, grp.length);

        Arrays.sort(this.grp);
    }

    /**
     * @return Array of group members.
     */
    public UUID[] group() {
        return grp;
    }

    /**
     * @return Local group member ID.
     */
    public UUID localId() {
        return locId;
    }

    /**
     * @return {@code true} if the group has a peer with the given {@code peerId}.
     */
    public boolean hasPeer(UUID peerId) {
        return Arrays.binarySearch(grp, peerId) >= 0;
    }
}
