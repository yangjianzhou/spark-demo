package com.iwill.spark.basic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    private static final Pattern TRAILING_DIGITS = Pattern.compile("[\\d]+$");
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter HR_FMT = DateTimeFormatter.ofPattern("HH");
    private static final Pattern PATTERN =
            Pattern.compile("\\{\\{([a-zA-Z0-9_\\-\\./]+)\\}\\}", Pattern.DOTALL);

    public static final String MILLIS = "millis";
    public static final String MICROS = "micros";

    private static final Map<String, Long> UNIT_FACTOR_MAP =
            ImmutableMap.of(MICROS, 1000 * 1000L, MILLIS, 1000L);

    private static String getDtHr(String dt, String hr, int offsetMins) {
        LocalDateTime localDtTime = LocalDateTime.parse(dt + hr + "0000", FMT);
        return localDtTime.plusMinutes(offsetMins).format(FMT);
    }

    public static String format(String value, LocalDateTime localDateTime) {
        String dt = DT_FMT.format(localDateTime);
        String hr = HR_FMT.format(localDateTime);
        Map<String, String> map = new HashMap<>();
        map.put("dt", dt);
        map.put("hr", hr);
        Matcher m = PATTERN.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            String v = map.get(key);
            m.appendReplacement(sb, v == null ? "" : v);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static final Pattern PATTERN_1 = Pattern.compile("[0-9]{12}", Pattern.DOTALL);

    public static Long getVersionFromVenusPath(String venusPath) {
        if (venusPath != null) {
            Matcher m = PATTERN_1.matcher(venusPath);
            if (m.find()) {
                return Long.valueOf(m.group());
            }
        }
        return null;
    }


    private static final DateTimeFormatter KST_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    public static Long kstDateTime(long epocTime, String unit){
        try {
            return Long.valueOf(
                    kstDateTime(
                            UNIT_FACTOR_MAP.containsKey(unit) ? epocTime / UNIT_FACTOR_MAP.get(unit) : epocTime));
        } catch (NumberFormatException n) {
            return null;
        }
    }

    public static Long findNextSmallest(long needle, List<Long> haystack) {
        if (haystack != null && !haystack.isEmpty()) {
            int haystackSize = haystack.size();
            return binarySearch(needle, haystack, 0, haystackSize - 1);
        }
        return 0L;
    }

    private static Long binarySearch(long needle, List<Long> haystack, int left, int right) {
        if (needle < haystack.get(left)) {
            if ((left - 1 >= 0)) {
                return haystack.get(left - 1);
            } else {
                return haystack.get(left);
            }
        }
        if (needle >= haystack.get(right)) {
            return haystack.get(right);
        }
        if (right >= left) {
            int mid = (left + right) / 2;
            long middleElement = haystack.get(mid);
            if (needle == middleElement) {
                return needle;
            } else if (needle > middleElement) {
                return binarySearch(needle, haystack, mid + 1, right);
            } else {
                return binarySearch(needle, haystack, left, mid - 1);
            }
        }
        return 0L;
    }


    private static String kstDateTime(long epocTime) {
        if (epocTime == 0) {
            return null;
        } else {
            return KST_FORMAT.format(Instant.ofEpochSecond(epocTime).atZone(KST));
        }
    }

    public static long getTime(Long needle,List<Long> haystack){
        if (needle != null && haystack != null && haystack.size() > 0) {
            List<Long> catList = new ArrayList<Long>(haystack);
            needle = kstDateTime(needle, "millis");
            return findNextSmallest(needle, catList);
        }
        return 0L;
    }

    public static void main(String[] args) {
     /*   System.out.println(getDtHr("20220415","10",0));
        System.out.println(format("venus/snapshot/prod/da_ads_parquet/dt={{dt}}{{hr}}" ,LocalDateTime.parse("20220415100000", FMT)));
        System.out.println(format("" ,LocalDateTime.parse("20220415100000", FMT)));*/
        //System.out.println(getVersionFromVenusPath("dt=202204100005/"));
        Long needle = 1649984496669L;
        List<Long> haystack = Lists.newArrayList(202204100005L, 202204101234L);
        System.out.println(getTime(needle ,haystack));
    }

}
