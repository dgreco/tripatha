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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.tripatha.loader.gen.EntityGenerator;
import org.tripatha.store.DatatypeMarkers;
import org.tripatha.store.EvalStatsMutations;
import org.tripatha.store.EvalStatsMutations.CounterPolicy;

/** Which counter cells a bulk build leaves out: of the whole store, or of one part of it. */
class BulkBuildLeftOutTest {

    private static final CounterPolicy SKIP = CounterPolicy.SKIP_SUBJECT_ONES;

    private static byte[] b(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void theStoresTotalsOfOneStayOutInEveryCounterFamilyButPWhenAlone() {
        for (final String family : EvalStatsMutations.COUNTER_FAMILIES) {
            assertEquals(!family.equals("p"), BulkBuild.leftOut(b(family), 1, 1, false, SKIP, false), family);
            // Next to another label's cell, or counted by the reference data too: the 1 stays.
            assertFalse(BulkBuild.leftOut(b(family), 1, 2, false, SKIP, false), family);
            assertFalse(BulkBuild.leftOut(b(family), 1, 1, true, SKIP, false), family);
            assertFalse(BulkBuild.leftOut(b(family), 1, 1, false, CounterPolicy.ALL, false), family);
            assertFalse(BulkBuild.leftOut(b(family), 2, 1, false, SKIP, false), family);
            assertTrue(BulkBuild.leftOut(b(family), 0, 0, false, CounterPolicy.ALL, false), family);
        }
        assertFalse(BulkBuild.leftOut(b(DatatypeMarkers.FAMILY), 1, 1, false, SKIP, false));
    }

    @Test
    void aPartsTotalsOfOneStayOutOnlyForSubjects() {
        // Another part may add to an object's or a pair's 1; a subject's counters are all in one entity.
        for (final String family : EvalStatsMutations.COUNTER_FAMILIES) {
            final boolean subject = List.of("s", "sp", "so").contains(family);
            assertEquals(subject, BulkBuild.leftOut(b(family), 1, 1, false, SKIP, true), family);
            assertFalse(BulkBuild.leftOut(b(family), 1, 2, false, SKIP, true), family);
            assertFalse(BulkBuild.leftOut(b(family), 1, 1, false, CounterPolicy.ALL, true), family);
            assertTrue(BulkBuild.leftOut(b(family), 0, 0, false, SKIP, true), family);
        }
        assertFalse(BulkBuild.leftOut(b(DatatypeMarkers.FAMILY), 1, 1, false, SKIP, true));
        assertFalse(BulkBuild.leftOut(b("n"), 1, 1, false, SKIP, true));
    }

    /** A sorted range of cells, decided one counter at a time. */
    @Test
    void aGroupDecidesEachCounterOverItsLabelCells() {
        final String elsewhere = BulkBuild.counterRead(b("Ceclon"), b("o"), b(""));
        final List<String> kept = new ArrayList<>();
        final BulkBuild.CounterGroup<String> group = new BulkBuild.LeftOut(Set.of(elsewhere), SKIP, false).group(kept::add);
        // Counter, label, total: one 1 alone; a 1 next to a 3; two 1s; a 1 the reference data counts; a 0.
        final String[][] cells = {
            {"alone", "o", "", "", "1"},
            {"pair", "o", "", "", "3"},
            {"pair", "o", "", "A", "1"},
            {"twos", "o", "g", "A", "1"},
            {"twos", "o", "g", "B", "1"},
            {"Ceclon", "o", "", "", "1"},
            {"zero", "o", "", "", "0"},
            {"zero", "o", "", "A", "1"},
            {"pred", "p", "", "", "1"},
        };
        for (final String[] c : cells) {
            group.add(b(c[0]), b(c[1]), b(c[2]), Long.parseLong(c[4]), String.join("|", c));
        }
        group.flush();
        // Counter|family|graph|label|total. The zero's counter has one cell other than 0, so its 1 is alone and stays out.
        assertEquals(List.of("pair|o|||3", "pair|o||A|1", "twos|o|g|A|1", "twos|o|g|B|1", "Ceclon|o|||1", "pred|p|||1"), kept);
    }

    @Test
    void theReferenceDataCountsItsLabelWords() {
        final Set<String> counted = BulkBuild.referenceCounters(new EntityGenerator(
                LoaderOptions.parse(new String[] {"load", "--table-prefix", "t_", "--entities", "100"}, java.util.Map.of()).genConfig()));
        assertFalse(counted.isEmpty());
        // The rdfs:label of every category, and its object word, are counted (all graphs, no label).
        assertTrue(counted.stream().anyMatch(k -> k.startsWith(java.util.HexFormat.of().formatHex(b("http://www.w3.org/2000/01/rdf-schema#label")))));
        assertTrue(counted.stream().allMatch(k -> k.split(":", -1).length == 3));
    }
}
