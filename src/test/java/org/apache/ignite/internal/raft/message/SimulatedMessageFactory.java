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
public class SimulatedMessageFactory implements MessageFactory {
    /** {@inheritDoc} */
    @Override public VoteRequest createVoteRequest(
        long term,
        UUID requestorId,
        long lastLogIdx,
        long lastLogTerm
    ) {
        return new SimulatedVoteRequest(
            term,
            requestorId,
            lastLogIdx,
            lastLogTerm
        );
    }

    /** {@inheritDoc} */
    @Override public VoteResponse createVoteResponse(long term, boolean voteGranted) {
        return new SimulatedVoteResponse(
            term,
            voteGranted
        );
    }

    /** {@inheritDoc} */
    @Override public AppendEntriesRequest createAppendEntriesRequest(
        long term,
        long lastLogIdx,
        long logTerm,
        LogEntry[] entries,
        long commitIdx
    ) {
        return new SimulatedAppendEntriesRequest(
            term,
            lastLogIdx,
            logTerm,
            entries,
            commitIdx);
    }

    /** {@inheritDoc} */
    @Override public RaftMessage createAppendEntriesResponse(
        long term,
        boolean success,
        long matchIdx,
        long commitIdx,
        long conflictStart,
        long conflictIdx,
        long conflictTerm
    ) {
        return new SimulatedAppendEntriesResponse(
            term,
            success,
            matchIdx,
            commitIdx,
            conflictStart,
            conflictIdx,
            conflictTerm
        );
    }

    /** {@inheritDoc} */
    @Override public <T> LogEntry createLogEntry(T data, long term, long idx) {
        return new SimulatedLogEntry(data, term, idx);
    }
}
