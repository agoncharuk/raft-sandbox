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

import java.util.Comparator;

/**
 *
 */
class RemoteFollowerState {
    /**
     *
     */
    private long nextIdx;

    /**
     * Match index for remote node
     * If -1, the node did not report a successfull match yet.
     * If greater or equal to 0, contains the index of the highest known replicated entry (with 0 being no entries).
     */
    private long matchIdx = -1;

    /**
     * Known commit index reported by the remote node.
     */
    private long knownCommitIdx;

    /**
     * Response deadline for a request sent from leader.
     */
    private long resDeadline;

    /**
     * Next heartbeat send deadline.
     */
    private long heartbeatDeadline;

    /**
     * Comparator comparing match indexes of the follower states in descending order.
     */
    public static final Comparator<RemoteFollowerState> MATCH_INDEX_COMPARATOR = new Comparator<RemoteFollowerState>() {
        @Override public int compare(RemoteFollowerState o1, RemoteFollowerState o2) {
            return Long.compare(o1.matchIdx, o2.matchIdx);
        }
    };

    /**
     * @param nextIdx Initial index to send equal to {@code lastLogIndex + 1} on leader.
     */
    RemoteFollowerState(long nextIdx) {
        this.nextIdx = nextIdx;
    }

    /**
     * @param success Whether remote follower acknowledged the log match.
     * @param matchIdx Largest known index for which the log is known to be matched
     *      (therefore, the next entry to replicate should be {@code matchIdx + 1}).
     * @param commitIdx Known commit index of remote follower. Used to eagerly send heartbeats to
     *      advance commit index.
     * @param divergenceBound Smallest index for which the log is known to be diverged
     *      (therefore, the next entry to replicate should be {@code divergenceBound}).
     * @return {@code true} if the state changed as a result of this call.
     */
    public boolean updateNextEntries(
        boolean success,
        long matchIdx,
        long commitIdx,
        long divergenceBound
    ) {
        if (success) {
            assert matchIdx >= 0;

            resDeadline = 0;

            if (commitIdx > knownCommitIdx)
                knownCommitIdx = commitIdx;

            if (matchIdx > this.matchIdx) {
                this.matchIdx = matchIdx;

                nextIdx = matchIdx + 1;

                return true;
            }
        }
        else {
            if (this.matchIdx == -1) {
                if (divergenceBound < nextIdx) {
                    nextIdx = divergenceBound;

                    resDeadline = 0;

                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @return {@code true} if remote follower sent a success response with match index.
     */
    public boolean matched() {
        return matchIdx != -1;
    }

    /**
     * @return Next log entry index leader will attempt to replicate to remote follower.
     */
    public long nextIndex() {
        return nextIdx;
    }

    /**
     * Records a remote node request start.
     *
     * @param resDeadline Deadline when the response is due.
     * @param heartbeatDeadline Next heartbeat deadline.
     */
    public void startRequest(long resDeadline, long heartbeatDeadline) {
        this.heartbeatDeadline = heartbeatDeadline;
        this.resDeadline = resDeadline;
    }

    /**
     */
    public boolean hearbeatDue(long now) {
        return heartbeatDeadline <= now;
    }

    /**
     */
    public boolean waitingResponse(long now) {
        return resDeadline != 0 && resDeadline > now;
    }

    /**
     * @return Log index known to be matched on remote node.
     */
    public long matchIndex() {
        return matchIdx;
    }

    /**
     * @return Known commit index for remote node.
     */
    public long knownCommitIndex() {
        return knownCommitIdx;
    }
}
