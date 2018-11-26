package fi.hsl.pulsar.monitoring.pipeline.impl;

import org.junit.Test;

import java.time.LocalTime;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class MessageCounterTest {
    @Test
    public void testBetweenWhenStartLargerThanEnd() {
        LocalTime start = LocalTime.parse("12:00:00");
        LocalTime end = LocalTime.parse("13:00:00");

        LocalTime t0 = LocalTime.parse("11:59:59");
        LocalTime t1 = LocalTime.parse("12:00:00");
        LocalTime t2 = LocalTime.parse("12:01:00");
        LocalTime t3 = LocalTime.parse("13:00:00");
        LocalTime t4 = LocalTime.parse("13:00:01");

        assertFalse(MessageCounter.isBetween(t0, start, end));
        assertFalse(MessageCounter.isBetween(t1, start, end));
        assertTrue(MessageCounter.isBetween(t2, start, end));
        assertFalse(MessageCounter.isBetween(t3, start, end));
        assertFalse(MessageCounter.isBetween(t4, start, end));

    }

    @Test
    public void testBetweenWhenEndLargerThanStart() {
        LocalTime start = LocalTime.parse("06:00:00");
        LocalTime end = LocalTime.parse("02:00:00");

        LocalTime t0 = LocalTime.parse("00:00:00");
        LocalTime t1 = LocalTime.parse("01:59:59");
        LocalTime t2 = LocalTime.parse("02:00:00");
        LocalTime t3 = LocalTime.parse("02:01:00");
        LocalTime t4 = LocalTime.parse("05:59:59");
        LocalTime t5 = LocalTime.parse("06:00:00");
        LocalTime t6 = LocalTime.parse("06:00:01");
        LocalTime t7 = LocalTime.parse("23:59:59");

        assertTrue(MessageCounter.isBetween(t0, start, end));
        assertTrue(MessageCounter.isBetween(t1, start, end));
        assertFalse(MessageCounter.isBetween(t2, start, end));
        assertFalse(MessageCounter.isBetween(t3, start, end));
        assertFalse(MessageCounter.isBetween(t4, start, end));
        assertFalse(MessageCounter.isBetween(t5, start, end));
        assertTrue(MessageCounter.isBetween(t6, start, end));
        assertTrue(MessageCounter.isBetween(t7, start, end));

    }
}
