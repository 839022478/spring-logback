package com.hyt.springboot.logback;

public class MySQLBuilder {

    //生成插入日志的sql
    static String buildInsertSQL() {
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append("log_records").append(" (");
        sqlBuilder.append("user_name").append(", ");
        sqlBuilder.append("pass_word").append(", ");
        sqlBuilder.append("app_key").append(", ");
        sqlBuilder.append("server_ip").append(", ");
        sqlBuilder.append("url").append(", ");
        sqlBuilder.append("request_ip").append(", ");
        sqlBuilder.append("project").append(", ");
        sqlBuilder.append("class").append(", ");
        sqlBuilder.append("classpath").append(", ");
        sqlBuilder.append("method").append(", ");
        sqlBuilder.append("thread_name").append(", ");
        sqlBuilder.append("msg_level").append(", ");
        sqlBuilder.append("msg").append(", ");
        sqlBuilder.append("create_date").append(") ");
        sqlBuilder.append("VALUES (?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        return sqlBuilder.toString();
    }

}
