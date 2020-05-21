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

import java.util.UUID;

/**
 * Volatile state maintained by replicator in FOLLOWER state.
 */
class Follower implements ReplicatorState {
    /** */
    private final long term;

    /** */
    private UUID leaderId;

    /** */
    private long electionDeadline;

    /**
     */
    public Follower(
        long curTerm,
        long electionDeadline
    ) {
        term = curTerm;
        this.electionDeadline = electionDeadline;
    }

    /** {@inheritDoc} */
    @Override public ReplicatorRole role() {
        return ReplicatorRole.FOLLOWER;
    }

    /**
     */
    public long electionDeadline() {
        return electionDeadline;
    }

    /**
     */
    public void electionDeadline(long deadline) {
        electionDeadline = deadline;
    }

    /**
     * @return Follower term.
     */
    public long term() {
        return term;
    }

    /** {@inheritDoc} */
    @Override public UUID leaderId() {
        return leaderId;
    }

    /**
     */
    public void leaderId(UUID leaderId) {
        if (this.leaderId != null && !this.leaderId.equals(leaderId))
            throw new UnrecoverableException("Attempted to change leader for the same term " +
                "[old=" + this.leaderId + ", new=" + leaderId + ']');

        this.leaderId = leaderId;
    }
}
