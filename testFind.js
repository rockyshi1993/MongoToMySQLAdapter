"use strict";

// 引入转换模块中的 MongoQueryBuilder 以及其他必要接口（确保 mongo2mysql.js 与该文件在同一目录下）
const { MongoQueryBuilder, SQLGenerationError } = require('./index');

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

    // ------------------------- 查询测试 -------------------------

    // Test Q1: 简单查询
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ username: 'john_doe' });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q1 - 简单查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q1 - 简单查询失败");
        failedTests++;
    }

    // Test Q2: 字段筛选（只返回特定字段）
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ username: 'alice' }).project(['username', 'email']);
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q2 - 字段筛选查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q2 - 字段筛选查询失败");
        failedTests++;
    }

    // Test Q3: 分页查询（limit 和 skip）
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ active: true }).limit(10).skip(20);
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q3 - 分页查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q3 - 分页查询失败");
        failedTests++;
    }

    // Test Q4: 排序查询
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ active: true }).sort({ age: -1 });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q4 - 排序查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q4 - 排序查询失败");
        failedTests++;
    }

    // Test Q5: 排序和分页查询
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ active: true }).sort({ age: 1 }).limit(5).skip(0);
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q5 - 排序与分页查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q5 - 排序与分页查询失败");
        failedTests++;
    }

    // Test Q6: 多条件查询（$and 和 $or）
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({
            $and: [
                { age: { $gte: 30 } },
                { role: 'manager' }
            ]
        });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q6 - 多条件查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q6 - 多条件查询失败");
        failedTests++;
    }

    // Test Q7: 正则表达式查询（模糊匹配）
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ username: { $regex: '^john' } });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q7 - 正则表达式查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q7 - 正则表达式查询失败");
        failedTests++;
    }

    // Test Q8: 查询数组字段（$in 操作符）
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ tags: { $in: ['developer', 'js'] } });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q8 - 查询数组字段通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q8 - 查询数组字段失败");
        failedTests++;
    }

    // Test Q9: 查询不存在的字段
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ non_existent_field: 'value' });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q9 - 查询不存在的字段通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q9 - 查询不存在的字段失败");
        failedTests++;
    }

    // Test Q10: 查询并使用多个字段（投影）
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ active: true }).project(['username', 'email', 'age']);
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q10 - 查询多个字段通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q10 - 查询多个字段失败");
        failedTests++;
    }

    // Test Q11: 聚合查询（如 $sum）
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ role: 'staff' }).project(['$sum', 'salary']);
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q11 - 聚合查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q11 - 聚合查询失败");
        failedTests++;
    }

    // Test Q12: 数值范围查询（$gt, $lt, $gte, $lte）
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ age: { $gte: 30, $lt: 40 } });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q12 - 数值范围查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q12 - 数值范围查询失败");
        failedTests++;
    }

    // Test Q13: 使用 $in 查询多个值
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ role: { $in: ['admin', 'manager'] } });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q13 - 使用 $in 查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q13 - 使用 $in 查询失败");
        failedTests++;
    }

    // Test Q14: 使用 $exists 查询字段是否存在
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ email: { $exists: true } });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q14 - 使用 $exists 查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q14 - 使用 $exists 查询失败");
        failedTests++;
    }

    // Test Q15: 分页与排序结合查询
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder
            .query({ active: true })
            .sort({ username: 1 })  // 排序
            .limit(5)               // 限制条数
            .skip(10);              // 偏移量

        let result = await queryBuilder.toSQL();
        console.log("SQL:", result.sql);
        logSuccess("✔ Test Q15 - 分页与排序查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q15 - 分页与排序查询失败");
        failedTests++;
    }

    // Test Q16: 使用 $size 查询数组长度
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ tags: { $size: 2 } });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q16 - 使用 $size 查询数组长度通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q16 - 使用 $size 查询数组长度失败");
        failedTests++;
    }

    // Test Q17: 排序多个字段
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ active: true }).sort({ age: -1, username: 1 });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q17 - 排序多个字段查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q17 - 排序多个字段查询失败");
        failedTests++;
    }

    // Test Q18: 使用嵌套字段查询
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ 'address.city': 'New York' });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q18 - 使用嵌套字段查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q18 - 使用嵌套字段查询失败");
        failedTests++;
    }

    // Test Q19: 查询包含 JSON 数据类型字段
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ preferences: { color: 'blue' } });
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q19 - 查询 JSON 数据类型字段通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q19 - 查询 JSON 数据类型字段失败");
        failedTests++;
    }

    // Test Q20: 联接查询
    try {
        const queryBuilder = new MongoQueryBuilder('users');
        queryBuilder.query({ role: 'admin' }).join([
            { table: 'orders', on: 'users.id = orders.user_id' }
        ]);
        let result = await queryBuilder.toSQL();
        logSuccess("✔ Test Q20 - 联接查询通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test Q20 - 联接查询失败");
        failedTests++;
    }

    // ------------------------- 测试结果 -------------------------
    console.log("\n测试结果总结:");
    console.log(`通过的测试: ${passedTests}`);
    console.log(`失败的测试: ${failedTests}`);
})();
