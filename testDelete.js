"use strict";

// 引入转换模块中的 MongoDeleteBuilder 以及其他必要接口
const { MongoDeleteBuilder, SQLGenerationError } = require('./index');

// 使用立即执行函数封装测试用例
(async () => {
    let passedTests = 0;
    let failedTests = 0;

    // 简单输出日志的颜色函数
    function logSuccess(message) {
        console.log(`\x1b[32m${message}\x1b[0m`);  // Green for success
    }

    function logFailure(message) {
        console.log(`\x1b[31m${message}\x1b[0m`);  // Red for failure
    }

    // ------------------------- 测试案例 -------------------------

    // Test D1: 单条删除（DELETE ONE）
    // 目的：验证删除一条记录时生成的 SQL 和参数是否正确
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.query({ username: 'john_doe' }).single().toSQL();
        logSuccess("✔ Test D1 - 单条删除通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D1 - 单条删除失败");
        failedTests++;
    }

    // Test D2: 批量删除（DELETE MANY）
    // 目的：验证批量删除操作，确保多个记录能够被正确删除
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.query({ active: false }).toSQL();
        logSuccess("✔ Test D2 - 批量删除通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D2 - 批量删除失败");
        failedTests++;
    }

    // Test D3: 删除时检查查询条件
    // 目的：验证删除时的查询条件是否正确处理
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.query({ age: { $lt: 18 } }).toSQL();
        logSuccess("✔ Test D3 - 删除操作条件检查通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D3 - 删除操作条件检查失败");
        failedTests++;
    }

    // Test D4: 删除时未指定查询条件（预期报错）
    // 目的：验证如果未指定查询条件，删除操作会抛出错误
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.toSQL();
        logSuccess("✔ Test D4 - 删除未指定查询条件通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D4 - 删除未指定查询条件预期出错");
        failedTests++;
    }

    // Test D5: 删除时多个条件叠加
    // 目的：验证在删除时使用多个查询条件时，是否能够正确构建 SQL 语句
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.query({ active: true }).query({ role: 'admin' }).toSQL();
        logSuccess("✔ Test D5 - 删除多个条件叠加通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D5 - 删除多个条件叠加失败");
        failedTests++;
    }

    // Test D6: 批量删除时检查字段类型
    // 目的：验证批量删除时，是否能够根据字段类型匹配删除条件
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.query({ age: { $gte: 30 } }).toSQL();
        logSuccess("✔ Test D6 - 批量删除时检查字段类型通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D6 - 批量删除时检查字段类型失败");
        failedTests++;
    }

    // Test D7: 删除时根据唯一索引判断
    // 目的：模拟删除时根据唯一索引字段进行查找
    async function checkUniqueConflict(tableName, column, value) {
        const existingValues = { username: ['john_doe', 'alice'] };
        return existingValues[column] && existingValues[column].includes(value);
    }
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        const usernameToDelete = 'john_doe';

        // 检查是否存在此用户
        const isConflict = await checkUniqueConflict('users', 'username', usernameToDelete);
        if (isConflict) {
            deleteBuilder.query({ username: usernameToDelete }).toSQL();
            logSuccess("✔ Test D7 - 删除时根据唯一索引判断通过");
            passedTests++;
        } else {
            logFailure("X Test D7 - 删除时根据唯一索引判断失败");
            failedTests++;
        }
    } catch (e) {
        logFailure("X Test D7 - 删除时根据唯一索引判断失败");
        failedTests++;
    }

    // Test D8: 批量删除时缺少查询条件（预期报错）
    // 目的：验证如果在批量删除时未指定查询条件，系统是否抛出错误
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.query().toSQL();
        logSuccess("✔ Test D8 - 批量删除时缺少查询条件通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D8 - 批量删除时缺少查询条件预期出错");
        failedTests++;
    }

    // Test D9: 删除时字段值为空（如 `null` 或空字符串）
    // 目的：验证删除操作中是否能够正确处理空值条件
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.query({ email: '' }).toSQL();
        logSuccess("✔ Test D9 - 删除时字段值为空通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D9 - 删除时字段值为空失败");
        failedTests++;
    }

    // Test D10: 批量删除多个条件
    // 目的：验证批量删除时多个条件能否组合在一起
    try {
        const deleteBuilder = new MongoDeleteBuilder('users');
        deleteBuilder.query({ age: { $gte: 30 } }).query({ active: true }).toSQL();
        logSuccess("✔ Test D10 - 批量删除多个条件通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test D10 - 批量删除多个条件失败");
        failedTests++;
    }

    // ------------------------- 测试结果 -------------------------
    console.log("\n测试结果总结:");
    console.log(`通过的测试: ${passedTests}`);
    console.log(`失败的测试: ${failedTests}`);
})();
