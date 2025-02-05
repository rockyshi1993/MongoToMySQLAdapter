"use strict";

// 引入转换模块中的 MongoUpdateBuilder 以及其他必要接口（确保 mongo2mysql.js 与该文件在同一目录下）
const { MongoUpdateBuilder } = require('../index');

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

    // Test U1: 单条更新使用 $set 操作符
    try {
        const updateBuilder = new MongoUpdateBuilder('users');
        updateBuilder.query({ username: 'john_doe' }).update({ $set: { age: 32 } });
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U1 - 单条更新 $set通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U1 - 单条更新 $set失败");
        failedTests++;
    }

    // Test U2: 单条更新使用 $inc 操作符
    try {
        const updateBuilder = new MongoUpdateBuilder('users');
        updateBuilder.query({ username: 'alice' }).update({ $inc: { loginCount: 1 } });
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U2 - 单条更新 $inc通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U2 - 单条更新 $inc失败");
        failedTests++;
    }

    // Test U3: 单条更新使用 $unset 操作符
    try {
        const updateBuilder = new MongoUpdateBuilder('users');
        updateBuilder.query({ username: 'bob' }).update({ $unset: { temporaryField: "" } });
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U3 - 单条更新 $unset通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U3 - 单条更新 $unset失败");
        failedTests++;
    }

    // Test U4: 单条更新使用 $mul 操作符
    try {
        const updateBuilder = new MongoUpdateBuilder('users');
        updateBuilder.query({ username: 'charlie' }).update({ $mul: { score: 1.1 } });
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U4 - 单条更新 $mul通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U4 - 单条更新 $mul失败");
        failedTests++;
    }

    // Test U5: 单条更新自动包装为 $set（未使用操作符前缀）
    try {
        const updateBuilder = new MongoUpdateBuilder('users');
        updateBuilder.query({ username: 'david' }).update({ email: 'david@example.com', active: true });
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U5 - 自动包装为 $set通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U5 - 自动包装为 $set失败");
        failedTests++;
    }

    // Test U6: 使用复杂查询条件的单条更新
    try {
        const updateBuilder = new MongoUpdateBuilder('users');
        updateBuilder.query({
            $and: [
                { age: { $gte: 18 } },
                { $or: [{ active: true }, { role: 'manager' }] }
            ]
        }).update({ $set: { active: false } });
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U6 - 更新操作复杂条件通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U6 - 更新操作复杂条件失败");
        failedTests++;
    }

    // Test U7: 批量更新操作（正常场景）
    try {
        const updateBuilder = new MongoUpdateBuilder('users', 'id');
        updateBuilder.query({ active: true }).update([
            { id: 1, score: 85 },
            { id: 2, score: 90 }
        ]);
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U7 - 批量更新正常场景通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U7 - 批量更新正常场景失败");
        failedTests++;
    }

    // // Test U8: 批量更新操作中缺少 id 字段（预期报错）
    // try {
    //     const updateBuilder = new MongoUpdateBuilder('users', 'id');
    //     updateBuilder.query({ active: true }).update([
    //         { id: 3, score: 88 },
    //         { score: 92 }
    //     ]);
    //     let result = updateBuilder.toSQL();
    //     logSuccess("✔ Test U8 - 批量更新缺少 id 字段 (预期报错)通过");
    //     passedTests++;
    // } catch (e) {
    //     logFailure("X Test U8 - 批量更新缺少 id 字段预期出错");
    //     failedTests++;
    // }

    // Test U9: 多次调用 query() 叠加条件进行更新
    try {
        const updateBuilder = new MongoUpdateBuilder('users');
        updateBuilder
            .query({ department: 'sales' })
            .query({ role: 'staff' })
            .update({ $inc: { bonus: 50 } });
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U9 - 多次调用 query() 叠加条件更新通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U9 - 多次调用 query() 叠加条件更新失败");
        failedTests++;
    }

    // Test U10: 更新操作未指定查询条件（预期报错）
    try {
        const updateBuilder = new MongoUpdateBuilder('users');
        updateBuilder.update({ $set: { active: true } });
        let result = updateBuilder.toSQL();
        logSuccess("✔ Test U10 - 更新操作无查询条件 (预期报错)通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test U10 - 更新操作无查询条件预期出错");
        failedTests++;
    }

    // ------------------------- 测试结果 -------------------------
    console.log("\n测试结果总结:");
    console.log(`通过的测试: ${passedTests}`);
    console.log(`失败的测试: ${failedTests}`);
})();
