package com.hyt.springboot.logback;

import ch.qos.logback.classic.spi.CallerData;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.db.DBAppenderBase;
import ch.qos.logback.core.db.DBHelper;
import com.hyt.springboot.utils.Base64Util;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class MyDBAppender extends DBAppenderBase<ILoggingEvent> {

    protected static final Method GET_GENERATED_KEYS_METHOD;

    //插入sql
    protected String insertSQL;
    //id
    static final int ID_INDEX = 1;
    //用户名
    static final int USERNAME_INDEX = 2;
    //密码
    static final int PASSWORD_INDEX = 3;
    //应用的appkey
    static final int APPKEY_INDEX = 4;
    //请求接口的服务ip
    static final int SERVERIP_INDEX = 5;
    //请求地址
    static final int URL_INDEX = 6;
    //请求地址
    static final int REQUESTIP_INDEX = 7;
    //工程名
    static final int PROJECT_INDEX = 8;
    //类名
    static final int CLASS_INDEX = 9;
    //路径
    static final int CLASSPATH_INDEX = 10;
    //方法名
    static final int METHOD_INDEX = 11;
    //线程名
    static final int THREADNAME_INDEX = 12;
    //信息级别
    static final int MSGLEVEL_INDEX = 13;
    //日志信息
    static final int MSG_INDEX = 14;
    //创建时间
    static final int CREATEDATE_INDEX = 15;

    static final StackTraceElement EMPTY_CALLER_DATA = CallerData.naInstance();

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
        insertSQL = MySQLBuilder.buildInsertSQL();
        super.start();
    }


    @Override
    protected Method getGeneratedKeysMethod() {
        return GET_GENERATED_KEYS_METHOD;
    }

    @Override
    public String getInsertSQL() {
        return insertSQL;
    }


    @Override
    protected void subAppend(ILoggingEvent event, Connection connection, PreparedStatement insertStatement) throws Throwable {
        bindLoggingMyInfoWithPreparedStatement(insertStatement, event);
        bindLoggingEventWithInsertStatement(insertStatement, event);
        // This is expensive... should we do it every time?
        bindCallerDataWithPreparedStatement(insertStatement, event.getCallerData());
        int updateCount = insertStatement.executeUpdate();
        if (updateCount != 1) {
            addWarn("Failed to insert loggingEvent");
        }
    }

    @Override
    protected void secondarySubAppend(ILoggingEvent eventObject, Connection connection, long eventId) throws Throwable {

    }

    //安全验证及个性化的数据
    void bindLoggingMyInfoWithPreparedStatement(PreparedStatement stmt, ILoggingEvent event) throws SQLException {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        String account = Base64Util.decodeBase64(request.getHeader("account"));
        String passwd = Base64Util.decodeBase64(request.getHeader("passwd"));
        String ip = Base64Util.decodeBase64(request.getHeader("serverIp"));
        String appkey = request.getHeader("appkey");
        stmt.setString(USERNAME_INDEX, account);
        stmt.setString(PASSWORD_INDEX, passwd);
        stmt.setString(SERVERIP_INDEX, ip);
        stmt.setString(APPKEY_INDEX, appkey);
        stmt.setString(URL_INDEX, request.getRequestURL().toString());
        stmt.setString(REQUESTIP_INDEX, request.getRemoteAddr());
        stmt.setString(PROJECT_INDEX, "tmall-api");
        Date day = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        stmt.setString(CREATEDATE_INDEX, df.format(day));
    }

    void bindLoggingEventWithInsertStatement(PreparedStatement stmt, ILoggingEvent event) throws SQLException {
        stmt.setString(MSG_INDEX, event.getFormattedMessage());
        stmt.setString(MSGLEVEL_INDEX, event.getLevel().toString());
        stmt.setString(THREADNAME_INDEX, event.getThreadName());
    }

    void bindCallerDataWithPreparedStatement(PreparedStatement stmt, StackTraceElement[] callerDataArray) throws SQLException {

        StackTraceElement caller = extractFirstCaller(callerDataArray);

        stmt.setString(CLASS_INDEX, caller.getFileName());
        stmt.setString(CLASSPATH_INDEX, caller.getClassName());
        stmt.setString(METHOD_INDEX, caller.getMethodName());
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
