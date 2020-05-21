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

import org.apache.ignite.internal.raft.message.LogEntry;

/**
 *
 */
public interface PersistenceService {
    /**
     * @return Latest persisted hard state for the local node.
     */
    public HardState hardState();

    /**
     * @return Latest persisted configuration state for the local node.
     */
    public ConfigurationState confState();

    /**
     * Index of a last logged entry.
     */
    public long lastLogIndex();

    /**
     * @param idx Index to probe.
     * @return Term for the entry at the given index. Will return {@code 0} for index {@code 0}.
     * @throws IllegalArgumentException if an entry with the given index does not exist.
     */
    public long entryTerm(long idx);

    /**
     * Fetches log entries into the output array starting from {@code startIdx}.
     *
     * @return The actual number of entries fetched.
     */
    public int fetchEntries(long startIdx, LogEntry[] out);

    /**
     * Given the log index, fetches the range of the term which corresponds to the log index.
     *
     * @param logIdx Index for which term range is fetched.
     * @return Term range, which is the term number that corresponds to the given log index,
     *      index of a first entry with this term, and the given log index.
     */
    public TermRange fetchTermRange(long logIdx);
}
