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

import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 *
 */
public class TimeSourceImplTest {
    /**
     */
    public static Stream<Arguments> testTimeSourceBoundaries() {
        return Stream.of(
            Arguments.of(0.3f, 1000),
            Arguments.of(0.3f, 2000),
            Arguments.of(0.3f, 5000),
            Arguments.of(0.3f, 10000),

            Arguments.of(0.5f, 1000),
            Arguments.of(0.5f, 2000),
            Arguments.of(0.5f, 5000),
            Arguments.of(0.5f, 10000),

            Arguments.of(0.7f, 1000),
            Arguments.of(0.7f, 2000),
            Arguments.of(0.7f, 5000),
            Arguments.of(0.7f, 10000),

            Arguments.of(1.f, 1000),
            Arguments.of(1.f, 2000),
            Arguments.of(1.f, 5000),
            Arguments.of(1.f, 10000),

            Arguments.of(2.f, 1000),
            Arguments.of(2.f, 2000),
            Arguments.of(2.f, 5000),
            Arguments.of(2.f, 10000),

            Arguments.of(5.f, 1000),
            Arguments.of(5.f, 2000),
            Arguments.of(5.f, 5000),
            Arguments.of(5.f, 10000)
        );
    }

    /**
     */
    @MethodSource
    @ParameterizedTest
    public void testTimeSourceBoundaries(float variation, long timeout) {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed);

        SimulatedTimeSource src = new SimulatedTimeSource(0);
        TimeServiceImpl ts = new TimeServiceImpl(src, new Random(seed));

        for (int i = 0; i < 10; i++) {
            long now = src.currentTimeMillis();

            for (int j = 0; j < 100; j++) {
                long deadline = ts.nextRandomizedDeadline(timeout, variation);

                Assertions.assertTrue(deadline >= now + Math.min(timeout, timeout * variation));
                Assertions.assertTrue(deadline <= now + Math.max(timeout, timeout * variation), "deadline=" + deadline + ", now=" + now + ", timeout=" + timeout);
            }

            src.advanceTo(i * 1000);
        }
    }
}
