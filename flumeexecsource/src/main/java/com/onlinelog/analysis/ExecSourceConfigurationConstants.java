package com.onlinelog.analysis;

/**
 * 在编写每一个source的时候，都需要设置一下必须设置的参数。
 */
public class ExecSourceConfigurationConstants {

    /**
     * Should the exec'ed command restarted if it dies: : default false
     */
    public static final String CONFIG_RESTART = "restart";
    public static final boolean DEFAULT_RESTART = false;

    /**
     * Amount of time to wait before attempting a restart: : default 10000 ms
     */
    public static final String CONFIG_RESTART_THROTTLE = "restartThrottle";
    public static final long DEFAULT_RESTART_THROTTLE = 10000L;

    /**
     * Should stderr from the command be logged: default false
     */
    public static final String CONFIG_LOG_STDERR = "logStdErr";
    public static final boolean DEFAULT_LOG_STDERR = false;

    /**
     * Number of lines to read at a time
     */
    public static final String CONFIG_BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 20;

    /**
     * Amount of time to wait, if the buffer size was not reached, before
     * to data is pushed downstream: : default 3000 ms
     */
    public static final String CONFIG_BATCH_TIME_OUT = "batchTimeout";
    public static final long DEFAULT_BATCH_TIME_OUT = 3000L;

    /**
     * Charset for reading input
     */
    public static final String CHARSET = "charset";
    public static final String DEFAULT_CHARSET = "UTF-8";

    /**
     * Optional shell/command processor used to run command
     */
    public static final String CONFIG_SHELL = "shell";
}
