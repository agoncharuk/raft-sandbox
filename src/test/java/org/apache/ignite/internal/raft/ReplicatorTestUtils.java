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

import org.apache.ignite.internal.raft.message.LogEntry;
import org.apache.ignite.internal.raft.message.SimulatedLogEntry;

/**
 *
 */
public class ReplicatorTestUtils {
    /** */
    public static final long[] EMPTY_TERMS = new long[0];

    /**
     */
    public static StandaloneEnvironmentBuilder environment() {
        return new StandaloneEnvironmentBuilder();
    }

    /**
     */
    public static LogEntry[] entriesFromTerms(long... terms) {
        return entriesFromTermsWithOffset(0, terms);
    }

    /**
     */
    public static LogEntry[] entriesFromTermsWithOffset(long off, long... terms) {
        LogEntry[] res = new LogEntry[terms.length];

        for (int i = 0; i < terms.length; i++) {
            long term = terms[i];

            res[i] = new SimulatedLogEntry(i + 1, term, off + i + 1);
        }

        return res;
    }

    /**
     */
    public static LogEntry[] entriesFromIndexes(long... indexes) {
        LogEntry[] res = new LogEntry[indexes.length];

        for (int i = 0; i < indexes.length; i++) {
            long idx = indexes[i];

            res[i] = new SimulatedLogEntry(idx, idx, idx);
        }

        return res;
    }
}
