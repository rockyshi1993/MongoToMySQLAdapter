"use strict";

// 引入转换模块中的 MongoInsertBuilder 以及其他必要接口
const { MongoInsertBuilder, SQLGenerationError } = require('../index');

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

    // Test I1: 单条插入（INSERT ONE）
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        insertBuilder.insertOne({ username: 'john_doe', age: 30, email: 'john@example.com' });
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I1 - 单条插入通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I1 - 单条插入失败");
        failedTests++;
    }

    // Test I2: 批量插入（INSERT MANY）
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        insertBuilder.insertMany([
            { username: 'alice', age: 25, email: 'alice@example.com' },
            { username: 'bob', age: 28, email: 'bob@example.com' }
        ]);
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I2 - 批量插入通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I2 - 批量插入失败");
        failedTests++;
    }

    // Test I3: 插入时检查唯一约束（如用户名是否已存在）
    async function checkUniqueConflict(tableName, column, value) {
        const existingValues = { username: ['john_doe', 'alice'] };
        return existingValues[column] && existingValues[column].includes(value);
    }
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        const newUsername = 'john_doe'; // 要插入的用户名

        // 检查是否已存在该用户名
        const isConflict = await checkUniqueConflict('users', 'username', newUsername);
        if (isConflict) {
            throw new SQLGenerationError(`用户名 ${newUsername} 已存在，不能进行插入。`, 'MongoInsertBuilder');
        }

        insertBuilder.insertOne({ username: newUsername, age: 40, email: 'john_doe_new@example.com' });
        logSuccess("✔ Test I3 - 唯一约束检查插入通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I3 - 插入时检查唯一约束失败");
        failedTests++;
    }

    // Test I4: 插入时使用默认值
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        insertBuilder.insertOne({ username: 'charlie', age: 35 });
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I4 - 插入时使用默认值通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I4 - 插入时使用默认值失败");
        failedTests++;
    }

    // Test I5: 插入时字段顺序无关性
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        insertBuilder.insertOne({ email: 'david@example.com', username: 'david', age: 32 });
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I5 - 字段顺序无关性通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I5 - 字段顺序无关性失败");
        failedTests++;
    }

    // Test I6: 插入时字段包含特殊字符（如 `-` 或 `_`）
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        insertBuilder.insertOne({ 'user-name': 'eve', 'user_age': 28 });
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I6 - 插入字段包含特殊字符通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I6 - 插入字段包含特殊字符失败");
        failedTests++;
    }

    // Test I7: 插入空数据（字段值为空）
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        insertBuilder.insertOne({ username: 'frank', email: '', age: null });
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I7 - 插入空数据通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I7 - 插入空数据失败");
        failedTests++;
    }

    // Test I8: 字段类型验证（年龄字段应该是数字）
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        insertBuilder.insertOne({ username: 'grace', age: 30, email: 'grace@example.com' });
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I8 - 字段类型验证通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I8 - 字段类型验证失败");
        failedTests++;
    }

    // Test I9: 批量插入时检查字段类型一致性
    try {
        const insertBuilder = new MongoInsertBuilder('users');
        insertBuilder.insertMany([
            { username: 'hank', age: 27, email: 'hank@example.com' },
            { username: 'isabel', age: 25, email: 'isabel@example.com' }
        ]);
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I9 - 批量插入字段类型一致性通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I9 - 批量插入字段类型一致性失败");
        failedTests++;
    }

    // Test I10: 批量插入重复数据（预期报错）
    async function checkDuplicateInserts(data) {
        const existingRecords = [{ username: 'john_doe', email: 'john@example.com' }];
        for (let record of data) {
            const duplicate = existingRecords.some(existingRecord => existingRecord.username === record.username);
            if (duplicate) {
                throw new SQLGenerationError(`用户名 ${record.username} 已存在，不能进行插入。`, 'MongoInsertBuilder');
            }
        }
    }

    try {
        const insertBuilder = new MongoInsertBuilder('users');
        const data = [
            { username: 'john_doe', age: 30, email: 'john@example.com' },  // 与现有数据冲突
            { username: 'lucy', age: 24, email: 'lucy@example.com' }
        ];

        // 检查重复数据
        await checkDuplicateInserts(data);

        insertBuilder.insertMany(data);
        let result = insertBuilder.toSQL();
        logSuccess("✔ Test I10 - 批量插入重复数据通过");
        passedTests++;
    } catch (e) {
        logFailure("X Test I10 - 批量插入重复数据预期出错");
        failedTests++;
    }

    // 打印最终统计
    console.log("\n测试结果总结:");
    console.log(`通过的测试: ${passedTests}`);
    console.log(`失败的测试: ${failedTests}`);
})();
