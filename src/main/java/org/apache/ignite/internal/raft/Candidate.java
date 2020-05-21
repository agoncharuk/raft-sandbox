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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.raft.service.ConfigurationState;

/**
 * Volatile state maintained by replicator in CANDIDATE state.
 */
class Candidate implements ReplicatorState {
    /** A set of nodes that approved the vote. */
    private final Set<UUID> approvedVotes;

    /** */
    private final long voteTerm;

    /** Cluster configuration for which this election was started. */
    private final ConfigurationState confState;

    /** Election deadline. Never changes as a node must start a new election if timeouts. */
    private final long electionDeadline;

    /**
     * Constructs a new candidate state.
     *
     * @param voteTerm Vote term that candidate initiated.
     */
    public Candidate(long voteTerm, ConfigurationState confState, long electionDeadline) {
        this.voteTerm = voteTerm;
        this.confState = confState;
        this.electionDeadline = electionDeadline;

        approvedVotes = new HashSet<>();
    }

    /** {@inheritDoc} */
    @Override public ReplicatorRole role() {
        return ReplicatorRole.CANDIDATE;
    }

    /** {@inheritDoc} */
    @Override public UUID leaderId() {
        return null;
    }

    /**
     * @param term Term for which vote response is received.
     * @param nodeId Node ID that voted.
     */
    void onVoteAccepted(long term, UUID nodeId) {
        if (term == voteTerm)
            approvedVotes.add(nodeId);
    }

    /**
     * @return {@code true} if the node won election in the candidate campaign term.
     */
    public boolean wonElection() {
        return approvedVotes.size() >= RaftUtils.quorumSize(confState.group());
    }

    /**
     * @return Election deadline, after which a new election should be started.
     */
    public long electionDeadline() {
        return electionDeadline;
    }
}
