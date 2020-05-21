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

/**
 *
 */
public class SimulatedAppendEntriesResponse implements AppendEntriesResponse {
    /** */
    private final long term;

    /** */
    private final boolean success;

    /** */
    private final long matchIdx;

    /** Commit index recorded by the responder node. */
    private final long commitIdx;

    /** */
    private final long conflictStart;

    /** */
    private final long conflictIdx;

    /** */
    private final long conflictTerm;

    /**
     */
    public SimulatedAppendEntriesResponse(
        long term,
        boolean success,
        long matchIdx,
        long commitIdx,
        long conflictStart,
        long conflictIdx,
        long conflictTerm
    ) {
        this.term = term;
        this.success = success;
        this.matchIdx = matchIdx;
        this.commitIdx = commitIdx;
        this.conflictStart = conflictStart;
        this.conflictIdx = conflictIdx;
        this.conflictTerm = conflictTerm;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.APPEND_ENTRIES_RESPONSE;
    }

    /** {@inheritDoc} */
    @Override public long term() {
        return term;
    }

    /** {@inheritDoc} */
    @Override public boolean success() {
        return success;
    }

    /** {@inheritDoc} */
    @Override public long matchIndex() {
        return matchIdx;
    }

    /** {@inheritDoc} */
    @Override public long commitIndex() {
        return commitIdx;
    }

    /** {@inheritDoc} */
    @Override public long conflictStart() {
        return conflictStart;
    }

    /** {@inheritDoc} */
    @Override public long conflictIndex() {
        return conflictIdx;
    }

    /** {@inheritDoc} */
    @Override public long conflictTerm() {
        return conflictTerm;
    }
}
