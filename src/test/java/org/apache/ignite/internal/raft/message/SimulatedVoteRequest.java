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

package org.apache.ignite.internal.raft.message;

import java.util.UUID;

/**
 *
 */
public class SimulatedVoteRequest implements VoteRequest {
    /** */
    private final long term;

    /** */
    private final UUID candidateId;

    /** */
    private final long lastLogIdx;

    /** */
    private final long lastLogTerm;

    /**
     */
    public SimulatedVoteRequest(
        long term,
        UUID candidateId,
        long lastLogIdx,
        long lastLogTerm
    ) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIdx = lastLogIdx;
        this.lastLogTerm = lastLogTerm;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.VOTE_REQUEST;
    }

    /** {@inheritDoc} */
    @Override public long term() {
        return term;
    }

    /** {@inheritDoc} */
    @Override public UUID candidateId() {
        return candidateId;
    }

    /** {@inheritDoc} */
    @Override public long lastLogIndex() {
        return lastLogIdx;
    }

    /** {@inheritDoc} */
    @Override public long lastLogTerm() {
        return lastLogTerm;
    }
}
