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
 *
 */
public class ReplicatorConfig {
    /** */
    private long electionTimeout;

    /** */
    private long appendEntriesBatchSize;

    /**
     * @return Election timeout.
     */
    public long getElectionTimeout() {
        return electionTimeout;
    }

    /**
     * @param electionTimeout Election timeout.
     * @return {@code this} for chaining.
     */
    public ReplicatorConfig setElectionTimeout(long electionTimeout) {
        this.electionTimeout = electionTimeout;

        return this;
    }

    /**
     * TODO this should be measured in bytes instead.
     * @return Maximum number of entries to send in a single append entries request.
     */
    public long getAppendEntriesBatchSize() {
        return appendEntriesBatchSize;
    }

    /**
     * @param appendEntriesBatchSize Maximum number of entries to send in a single append entries request.
     * @return {@code this} for chaining.
     */
    public ReplicatorConfig setAppendEntriesBatchSize(long appendEntriesBatchSize) {
        this.appendEntriesBatchSize = appendEntriesBatchSize;

        return this;
    }
}
