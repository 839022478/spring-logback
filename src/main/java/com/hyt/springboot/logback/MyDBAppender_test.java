package com.hyt.springboot.logback;

import ch.qos.logback.classic.spi.*;
import ch.qos.logback.core.db.DBAppenderBase;
import ch.qos.logback.core.db.DBHelper;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @Description:
 * @Author: Rocty
 * @Date: 2019-06-18 16:22
 * @Version: 1.0
 */
public class MyDBAppender_test extends DBAppenderBase<ILoggingEvent> {

    private String insertSQL;
    private static final Method GET_GENERATED_KEYS_METHOD;

    private static final int TIME_INDEX = 1;
    private static final int MESSAGE_INDEX = 2;
    private static final int LEVEL_STRING_INDEX = 3;
    private static final int LOGGER_NAME_INDEX = 4;
    private static final int THREAD_NAME_INDEX = 5;
    private static final int CALLER_FILENAME_INDEX = 6;
    private static final int CALLER_CLASS_INDEX = 7;
    private static final int CALLER_METHOD_INDEX = 8;
    private static final int CALLER_LINE_INDEX = 9;
    private static final int ARG0_INDEX = 10;
    private static final int ARG1_INDEX = 11;
    private static final int ARG2_INDEX = 12;
    private static final int ARG3_INDEX = 13;

    private static final StackTraceElement EMPTY_CALLER_DATA = CallerData.naInstance();

    static {
        // PreparedStatement.getGeneratedKeys() method was added in JDK 1.4
        Method getGeneratedKeysMethod;
        try {
            // the
            getGeneratedKeysMethod = PreparedStatement.class.getMethod("getGeneratedKeys", (Class[]) null);
        } catch (Exception ex) {
            getGeneratedKeysMethod = null;
        }
        GET_GENERATED_KEYS_METHOD = getGeneratedKeysMethod;
    }

    @Override
    public void start() {
        insertSQL = buildInsertSQL();
        super.start();
    }

    private static String buildInsertSQL() {
        /*return "INSERT INTO \"LOG_RECORD\" (\"MESSAGE\", \"LEVEL_STRING\", \"LOGGER_NAME\", \"THREAD_NAME\", \"REFERENCE_FLAG\", \"CALLER_FILENAME\", \"CALLER_CLASS\", \"CALLER_METHOD\", \"CALLER_LINE\") " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); ";*/
        return "INSERT INTO log_record " +
                "(time, message, level_string, logger_name, thread_name," +
                "caller_filename, caller_class, caller_method, caller_line, arg0, arg1, arg2, arg3)" +
                "VALUES (?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private void bindLoggingEventWithInsertStatement(PreparedStatement stmt, ILoggingEvent event) throws SQLException {
        stmt.setTimestamp(TIME_INDEX, new Timestamp(event.getTimeStamp()));
        stmt.setString(MESSAGE_INDEX, event.getFormattedMessage());
        stmt.setString(LEVEL_STRING_INDEX, event.getLevel().toString());
        stmt.setString(LOGGER_NAME_INDEX, event.getLoggerName());
        stmt.setString(THREAD_NAME_INDEX, event.getThreadName());
    }

    private void bindCallerDataWithPreparedStatement(PreparedStatement stmt, StackTraceElement[] callerDataArray) throws SQLException {
        StackTraceElement caller = extractFirstCaller(callerDataArray);
        stmt.setString(CALLER_FILENAME_INDEX, caller.getFileName());
        stmt.setString(CALLER_CLASS_INDEX, caller.getClassName());
        stmt.setString(CALLER_METHOD_INDEX, caller.getMethodName());
        stmt.setString(CALLER_LINE_INDEX, Integer.toString(caller.getLineNumber()));
    }

    @Override
    protected void subAppend(ILoggingEvent event, Connection connection, PreparedStatement insertStatement) throws Throwable {
        bindLoggingEventWithInsertStatement(insertStatement, event);


        // This is expensive... should we do it every time?
        bindCallerDataWithPreparedStatement(insertStatement, event.getCallerData());

        bindLoggingEventArgumentsWithPreparedStatement(insertStatement, event.getArgumentArray());

        int updateCount = insertStatement.executeUpdate();
        if (updateCount != 1) {
            addWarn("Failed to insert loggingEvent");
        }
    }

    void bindLoggingEventArgumentsWithPreparedStatement(PreparedStatement stmt, Object[] argArray) throws SQLException {

        int arrayLen = argArray != null ? argArray.length : 0;

        for (int i = 0; i < arrayLen && i < 4; i++) {
            stmt.setString(ARG0_INDEX + i, asStringTruncatedTo254(argArray[i]));
        }
        if (arrayLen < 4) {
            for (int i = arrayLen; i < 4; i++) {
                stmt.setString(ARG0_INDEX + i, null);
            }
        }
    }

    String asStringTruncatedTo254(Object o) {
        String s = null;
        if (o != null) {
            s = o.toString();
        }

        if (s == null) {
            return null;
        }
        if (s.length() <= 254) {
            return s;
        } else {
            return s.substring(0, 254);
        }
    }

    private StackTraceElement extractFirstCaller(StackTraceElement[] callerDataArray) {
        StackTraceElement caller = EMPTY_CALLER_DATA;
        if (hasAtLeastOneNonNullElement(callerDataArray))
            caller = callerDataArray[0];
        return caller;
    }

    private boolean hasAtLeastOneNonNullElement(StackTraceElement[] callerDataArray) {
        return callerDataArray != null && callerDataArray.length > 0 && callerDataArray[0] != null;
    }

    @Override
    protected Method getGeneratedKeysMethod() {
        return GET_GENERATED_KEYS_METHOD;
    }

    @Override
    protected String getInsertSQL() {
        return insertSQL;
    }

    protected void secondarySubAppend(ILoggingEvent event, Connection connection, long eventId) throws Throwable {
    }

    @Override
    public void append(ILoggingEvent eventObject) {
        Connection connection = null;
        try {
            connection = connectionSource.getConnection();
            connection.setAutoCommit(false);
            PreparedStatement insertStatement;
            insertStatement = connection.prepareStatement(getInsertSQL());
            // inserting an event and getting the result must be exclusive
            synchronized (this) {
                subAppend(eventObject, connection, insertStatement);
            }

            // we no longer need the insertStatement
            if (insertStatement != null) {
                insertStatement.close();
            }
            connection.commit();
        } catch (Throwable sqle) {
            addError("problem appending event", sqle);
        } finally {
            DBHelper.closeConnection(connection);
        }
    }
}
