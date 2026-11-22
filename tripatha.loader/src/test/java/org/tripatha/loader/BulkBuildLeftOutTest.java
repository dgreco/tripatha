/*
 * Copyright 2026 David Greco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tripatha.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.tripatha.store.DatatypeMarkers;
import org.tripatha.store.EvalStatsMutations;
import org.tripatha.store.EvalStatsMutations.CounterPolicy;

/** Which counter totals a bulk build leaves out: of the whole store, or of one part of it. */
class BulkBuildLeftOutTest {

    private static byte[] f(final String family) {
        return family.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void theStoresTotalsOfOneStayOutInEveryCounterFamilyButP() {
        for (final String family : EvalStatsMutations.COUNTER_FAMILIES) {
            assertEquals(!family.equals("p"), BulkBuild.leftOut(f(family), 1, CounterPolicy.SKIP_SUBJECT_ONES), family);
            assertEquals(false, BulkBuild.leftOut(f(family), 1, CounterPolicy.ALL), family);
            assertEquals(false, BulkBuild.leftOut(f(family), 2, CounterPolicy.SKIP_SUBJECT_ONES), family);
            assertEquals(true, BulkBuild.leftOut(f(family), 0, CounterPolicy.ALL), family);
        }
        assertEquals(false, BulkBuild.leftOut(f(DatatypeMarkers.FAMILY), 1, CounterPolicy.SKIP_SUBJECT_ONES));
    }

    @Test
    void aPartsTotalsOfOneStayOutOnlyForSubjects() {
        // Another part may add to an object's or a pair's 1; a subject's counters are all in one entity.
        for (final String family : EvalStatsMutations.COUNTER_FAMILIES) {
            final boolean subject = List.of("s", "sp", "so").contains(family);
            assertEquals(subject, BulkBuild.leftOutOfPart(f(family), 1, CounterPolicy.SKIP_SUBJECT_ONES), family);
            assertEquals(false, BulkBuild.leftOutOfPart(f(family), 1, CounterPolicy.ALL), family);
            assertEquals(false, BulkBuild.leftOutOfPart(f(family), 2, CounterPolicy.SKIP_SUBJECT_ONES), family);
            assertEquals(true, BulkBuild.leftOutOfPart(f(family), 0, CounterPolicy.SKIP_SUBJECT_ONES), family);
        }
        assertEquals(false, BulkBuild.leftOutOfPart(f(DatatypeMarkers.FAMILY), 1, CounterPolicy.SKIP_SUBJECT_ONES));
        assertEquals(false, BulkBuild.leftOutOfPart(f("n"), 1, CounterPolicy.SKIP_SUBJECT_ONES));
    }
}
