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

import java.util.Objects;

/**
 *
 */
public class SimulatedLogEntry implements LogEntry {
    /** */
    private final long term;

    /** */
    private final long idx;

    /** */
    private final Object data;

    /**
     */
    public <T> SimulatedLogEntry(T data, long term, long idx) {
        this.data = data;
        this.term = term;
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public long term() {
        return term;
    }

    /** {@inheritDoc} */
    @Override public <T> T data() {
        return (T)data;
    }

    /** {@inheritDoc} */
    @Override public long index() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "SimulatedLogEntry [term=" + term + ", idx=" + idx + ", data=" + data + ']';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof SimulatedLogEntry))
            return false;

        SimulatedLogEntry that = (SimulatedLogEntry)o;

        return term == that.term && idx == that.idx && Objects.equals(data, that.data);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(term, idx, data);
    }
}
