package org.poem.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Calendar;
import java.util.Date;

/**
 * @author poem
 */
public class DateUtils {

    private static final ThreadLocal<SimpleDateFormat> SIMPLE_DATE_TIME_FORMAT_THREAD_LOCAL;

    private static final ThreadLocal<SimpleDateFormat> SIMPLE_DATE_FORMAT_THREAD_LOCAL;

    private static final ThreadLocal<SimpleDateFormat> SIMPLE_MONTH_THREAD_LOCAL;

    private static final ThreadLocal<SimpleDateFormat> SIMPLE_YEAR_THREAD_LOCAL;

    private static final ThreadLocal<SimpleDateFormat> SIMPLE_TIME_THREAD_LOCAL;

    private static final ThreadLocal<SimpleDateFormat> SIMPLE_TIME_MINUTES_THREAD_LOCAL;


    public static final String DATE_TIME = "yyyy-MM-dd HH:mm:ss";

    public static final String DATE = "yyyy-MM-dd";

    public static final String MONTH = "yyyy-MM";

    public static final String YEAR = "yyyy";

    public static final String TIME = "HH:mm:ss";


    public static final String TIMEMIN = "HH:mm";

    private static int[] monthDays = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    private static Logger logger = LoggerFactory.getLogger(DateUtils.class);

    public static String toDateTimeString(Date date) {
        return date == null ? null : SIMPLE_DATE_TIME_FORMAT_THREAD_LOCAL.get().format(date);
    }

    public static String toDateString(Timestamp date) {
        return date == null ? null : SIMPLE_DATE_TIME_FORMAT_THREAD_LOCAL.get().format(date);
    }

    public static String toDate(Timestamp date) {
        return date == null ? null : SIMPLE_DATE_FORMAT_THREAD_LOCAL.get().format(date);
    }

    public static String toMonth(Timestamp date) {
        return date == null ? null : SIMPLE_MONTH_THREAD_LOCAL.get().format(date);
    }

    public static String toYear(Timestamp date) {
        return date == null ? null : SIMPLE_YEAR_THREAD_LOCAL.get().format(date);
    }

    public static String toTime(Timestamp date) {
        return date == null ? null : SIMPLE_TIME_THREAD_LOCAL.get().format(date);
    }

    public static String toHourMinutes(Timestamp data) {
        return data == null ? null : SIMPLE_TIME_MINUTES_THREAD_LOCAL.get().format(data);
    }

    /**
     * 获取周
     *
     * @param date
     * @return
     */
    public static int getYearWeek(Timestamp date) {

        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setTime(date);

        return calendar.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     * 1 第一季度 2 第二季度 3 第三季度 4 第四季度
     *
     * @param date
     * @return
     */
    public static int getSeason(Date date) {

        int season = 0;

        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int month = c.get(Calendar.MONTH);
        switch (month) {
            case Calendar.JANUARY:
            case Calendar.FEBRUARY:
            case Calendar.MARCH:
                season = 1;
                break;
            case Calendar.APRIL:
            case Calendar.MAY:
            case Calendar.JUNE:
                season = 2;
                break;
            case Calendar.JULY:
            case Calendar.AUGUST:
            case Calendar.SEPTEMBER:
                season = 3;
                break;
            case Calendar.OCTOBER:
            case Calendar.NOVEMBER:
            case Calendar.DECEMBER:
                season = 4;
                break;
            default:
                break;
        }
        return season;
    }

    /**
     * 获取当前时间
     *
     * @return
     */
    public static Timestamp getCurrentTime() {
        return new Timestamp(System.currentTimeMillis());
    }

    /**
     * 多线程
     */
    static {
        SIMPLE_DATE_TIME_FORMAT_THREAD_LOCAL = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(DATE_TIME);
            }
        };
        SIMPLE_TIME_THREAD_LOCAL = ThreadLocal.withInitial(() -> new SimpleDateFormat(TIME));
        SIMPLE_DATE_FORMAT_THREAD_LOCAL = ThreadLocal.withInitial(() -> new SimpleDateFormat(DATE));
        SIMPLE_MONTH_THREAD_LOCAL = ThreadLocal.withInitial(() -> new SimpleDateFormat(MONTH));
        SIMPLE_YEAR_THREAD_LOCAL = ThreadLocal.withInitial(() -> new SimpleDateFormat(YEAR));
        SIMPLE_TIME_MINUTES_THREAD_LOCAL = ThreadLocal.withInitial(() -> new SimpleDateFormat(TIMEMIN));
    }

    public static Timestamp formatTimestampDateTime(String datetime) {
        if (StringUtils.isEmpty(datetime)) {
            return null;
        }
        Date date = null;
        try {
            date = SIMPLE_DATE_TIME_FORMAT_THREAD_LOCAL.get().parse(datetime);
        } catch (ParseException e) {
            return new Timestamp(Long.valueOf(datetime.split(" ")[0]));
        }
        return date == null ? null : new Timestamp(date.getTime());
    }

    /**
     * 判断是否是今天
     *
     * @param time
     * @return
     */
    public static boolean isToday(String time) {
        //2018-01-12 14:42:18
        String day = time.substring(0, 10);
        // 格式化时间
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern(DATE);
        String newTime = sdf.format(new Date());
        return day.equals(newTime);
    }



    /**
     * 转化城数据
     *
     * @return
     */
    public static String formatDateTime(Object data) {
        if (data instanceof Date || data instanceof Timestamp) {
            Date endTimeDate = (Date) data;
            return DateUtils.toDateTimeString(endTimeDate);
        } else if (data instanceof String) {
            return String.valueOf(data).substring(0, 19);
        }
        return DateUtils.toDateTimeString(new Date());
    }

    /***
     * 获取当前时间前几分钟的时间
     * @param minute 分钟
     * @return
     */
    public static Date getBeforeNowMinute(int minute) {
        //获取当前时间前10分钟的时间
        Calendar beforeTime = Calendar.getInstance();
        // 10分钟之前的时间
        beforeTime.add(Calendar.MINUTE, -minute);
        return beforeTime.getTime();
    }

    /***
     * 获取当前时间前几天的时间
     * @param day 分钟
     * @return
     */
    public static LocalDateTime getBeforeNowDay(int day) {
        //获取当前时间前10分钟的时间
        Calendar beforeTime = Calendar.getInstance();
        // 10分钟之前的时间
        beforeTime.add(Calendar.DAY_OF_MONTH, -day);
        return LocalDateTime.from(new TemporalAccessor() {
            @Override
            public boolean isSupported(TemporalField field) {
                return true;
            }

            @Override
            public long getLong(TemporalField field) {
                return beforeTime.getTime().getTime();
            }
        });
    }

    public static String getBeforeNowDayStr(int day) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -day);
        Date d = cal.getTime();

         //获取昨天日期
        SimpleDateFormat sp = new SimpleDateFormat(DATE);
        return sp.format(d);
    }


    /***
     * 获取当前时间前几分钟的时间
     */
    public static Date getBeforeNow1Minute() {
        return getBeforeNowMinute(1);
    }

    /***
     * 获取当前时间前几分钟的时间
     * @return
     */
    public static Date getBeforeNow16Minute() {
        return getBeforeNowMinute(16);
    }

    public static String getMonitoryTime(Object time) {
        if (time == null) {
            return "";
        }
        int index = String.valueOf(time).indexOf(" ");
        if (index < 0) {
            return String.valueOf(time);
        }
        return String.valueOf(time).substring(index + 1);
    }

    /**
     * 是否是闰年
     *
     * @param year
     * @return
     */
    public static boolean isLeapYear(int year) {
        return (year % 4 == 0 && year % 100 != 0) || (year % 4 == 0 && year % 400 == 0);
    }

    /**
     * 获取昨天的时间
     *
     * @param today
     * @return
     */
    public static String yesterday(String today) {
        String[] dateArr = today.split("-");
        if ("01".equals(dateArr[2])) {
            if ("01".equals(dateArr[1])) {
                dateArr[2] = "31";
                dateArr[1] = "12";
                dateArr[0] = "" + (Integer.parseInt(dateArr[0]) - 1);
            } else if ("03".equals(dateArr[1])) {
                if (isLeapYear(Integer.parseInt(dateArr[0]))) {
                    dateArr[2] = "29";
                } else {
                    dateArr[2] = "28";
                }
                dateArr[1] = "02";
            } else {
                char m1 = dateArr[1].charAt(0);
                int m2 = 0;
                if (m1 == '1') {
                    m2 = Integer.parseInt(dateArr[1]);
                    dateArr[1] = m2 - 1 > 9 ? "" + (m2 - 1) : "0" + (m2 - 1);


                } else {
                    m2 = dateArr[1].charAt(1) - 48;
                    dateArr[1] = "0" + (m2 - 1);

                }
                dateArr[2] = "" + monthDays[m2 - 2];
            }
        } else {
            char m1 = dateArr[2].charAt(0);
            int m2 = 0;
            if (m1 != '0') {
                m2 = Integer.parseInt(dateArr[2]);
                dateArr[2] = (m2 - 1) < 10 ? "0" + (m2 - 1) : "" + (m2 - 1);
            } else {
                m2 = dateArr[2].charAt(1) - 48;
                dateArr[2] = "0" + (m2 - 1);
            }
        }

        return dateArr[0] + "-" + dateArr[1] + "-" + dateArr[2];
    }
}
