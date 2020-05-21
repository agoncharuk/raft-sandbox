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

package org.apache.ignite.internal.raft;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.raft.service.ConfigurationState;

/**
 * Volatile state maintained by replicator in LEADER state.
 */
class Leader implements ReplicatorState {
    /** */
    private static final RemoteFollowerState[] EMPTY_STATES = new RemoteFollowerState[0];

    /**
     * For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
     * and index of the highest log entry known to be replicated on that server (initialized to 0,
     * increases monotonically).
     */
    private final Map<UUID, RemoteFollowerState> remoteIndexes;

    /**
     * Term of this leader.
     */
    private final UUID locId;

    /** */
    private final int quorumSize;

    /**
     */
    public Leader(
        long term,
        ConfigurationState confState,
        long lastLogIdx
    ) {
        locId = confState.localId();

        remoteIndexes = Stream.of(confState.group())
            .collect(Collectors.toMap(id -> id, n -> new RemoteFollowerState(lastLogIdx + 1)));

        quorumSize = RaftUtils.quorumSize(confState.group());
    }

    /** {@inheritDoc} */
    @Override public ReplicatorRole role() {
        return ReplicatorRole.LEADER;
    }

    /** {@inheritDoc} */
    @Override public UUID leaderId() {
        return locId;
    }

    /**
     * @return Remote follower by {@code peerId}.
     */
    public RemoteFollowerState remoteFollower(UUID peerId) {
        return remoteIndexes.get(peerId);
    }

    /**
     * @return Index which is safe to commit.
     */
    public long effectiveCommitIndex() {
        // Check which index is confirmed by a majority of nodes.
        RemoteFollowerState[] states = remoteIndexes.values().toArray(EMPTY_STATES);

        Arrays.sort(states, RemoteFollowerState.MATCH_INDEX_COMPARATOR);

        // TODO this does not work for a cluster with 1 node.
        long idx = states[remoteIndexes.size() + 1 - quorumSize].matchIndex();

        return Math.max(idx, 0);
    }
}
