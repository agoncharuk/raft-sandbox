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

/**
 * Records the leader attempt to replicate a set of log entries. The receipt will contain the requested log indexes
 * for the entries and the leader term. Once an entry with the given index is committed, the requester may compare the
 * entry term with the receipt. If they match, the entries were committed; if not, the entries failed to commit.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AppendReceipt {
    /** Leader term. */
    private final long term;

    /** Indices assigned to the log entries. */
    private final long[] idxs;

    /**
     */
    public AppendReceipt(long term, long[] idxs) {
        this.term = term;
        this.idxs = idxs;
    }

    /**
     * @return Leader term with which the entries were proposed.
     */
    public long term() {
        return term;
    }

    /**
     * @return Entries indexes under which the commands were logged.
     */
    public long[] indexes() {
        return idxs;
    }
}
