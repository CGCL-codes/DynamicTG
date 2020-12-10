package tg.dtg.util;

import java.util.concurrent.TimeUnit;

public class Timer {
    private static long markTime;

    public static void mark() {
        markTime = System.nanoTime();
    }

    public static long nanos() {
        long tm = System.nanoTime();
        long t = tm - markTime;
        markTime = tm;
        return t;
    }

    public static long mills() {
        return TimeUnit.NANOSECONDS.toMillis(nanos());
    }
}
