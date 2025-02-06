"use strict";

// 引入转换模块中的 MongoQueryBuilder、MongoAggregationBuilder、MongoUpdateBuilder 等接口
const {
    MongoQueryBuilder,
    MongoAggregationBuilder,
    MongoUpdateBuilder,
    MongoDeleteBuilder,
    MongoInsertBuilder,
    SubQuery,
    mongoToMySQL,
    mongoToMySQLWithJoinsOptimized
} = require('../lib');

// 使用立即执行的异步函数封装测试用例
(async () => {
    let passedTests = 0;
    let failedTests = 0;

    // 简单输出日志的颜色函数
    function logSuccess(message) {
        console.log(`\x1b[32m${message}\x1b[0m`);  // Green for success
    }

    function logFailure(message, sql, params) {
        console.log(`\x1b[31m${message} SQL: ${sql}\x1b[0m`);  // Red for failure
    }

    // ------------------------- 插入测试 -------------------------

    // Test 1: 单条插入（INSERT ONE）
    try {
        const builder = new MongoInsertBuilder('users');
        builder.insertOne({ name: 'John Doe', age: 30 });
        let res = builder.toSQL();
        if (res.sql === "INSERT INTO users (name, age) VALUES (?, ?)") {
            logSuccess("✔ Test 1 - 单条插入通过");
            passedTests++;
        } else {
            logFailure("X Test 1 - 单条插入失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 1 出错：", e); failedTests++; }

    // Test 2: 批量插入（INSERT MANY）
    try {
        const builder = new MongoInsertBuilder('users');
        builder.insertMany([
            { name: 'Alice', age: 25 },
            { name: 'Bob', age: 28 }
        ]);
        let res = builder.toSQL();
        if (res.sql === "INSERT INTO users (name, age) VALUES (?, ?), (?, ?)") {
            logSuccess("✔ Test 2 - 批量插入通过");
            passedTests++;
        } else {
            logFailure("X Test 2 - 批量插入失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 2 出错：", e); failedTests++; }

    // Test 3: Upsert 插入（INSERT + ON DUPLICATE KEY UPDATE）
    try {
        const builder = new MongoInsertBuilder('users');
        builder.insertOne({ id: 1, name: 'Charlie', age: 40 }).upsert(true, ['name', 'age']);
        let res = builder.toSQL();
        if (res.sql === "INSERT INTO users (id, name, age) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), age = VALUES(age)") {
            logSuccess("✔ Test 3 - Upsert 插入通过");
            passedTests++;
        } else {
            logFailure("X Test 3 - Upsert 插入失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 3 出错：", e); failedTests++; }

    // Test 4: 插入测试：字段顺序验证
    try {
        const builder = new MongoInsertBuilder('users');
        builder.insertOne({ age: 22, name: 'David' });
        let res = builder.toSQL();
        if (res.sql === "INSERT INTO users (age, name) VALUES (?, ?)") {
            logSuccess("✔ Test 4 - 字段顺序插入通过");
            passedTests++;
        } else {
            logFailure("X Test 4 - 字段顺序插入失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 4 出错：", e); failedTests++; }

    // ------------------------- 更新测试 -------------------------

    // Test 5: 单条更新 $set
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'John Doe' }).update({ $set: { age: 31 } });
        let res = builder.toSQL();
        if (res.sql === "UPDATE users SET age = ? WHERE name = ?") {
            logSuccess("✔ Test 5 - 单条更新 $set通过");
            passedTests++;
        } else {
            logFailure("X Test 5 - 单条更新 $set失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 5 出错：", e); failedTests++; }

    // Test 6: 更新操作 $inc
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'Alice' }).update({ $inc: { age: 1 } });
        let res = builder.toSQL();
        if (res.sql === "UPDATE users SET age = age + ? WHERE name = ?") {
            logSuccess("✔ Test 6 - 更新 $inc通过");
            passedTests++;
        } else {
            logFailure("X Test 6 - 更新 $inc失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 6 出错：", e); failedTests++; }

    // Test 7: 更新操作 $unset
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'Bob' }).update({ $unset: { age: "" } });
        let res = builder.toSQL();
        if (res.sql === "UPDATE users SET age = NULL WHERE name = ?") {
            logSuccess("✔ Test 7 - 更新 $unset通过");
            passedTests++;
        } else {
            logFailure("X Test 7 - 更新 $unset失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 7 出错：", e); failedTests++; }

    // Test 8: 更新操作 $mul
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'Alice' }).update({ $mul: { age: 2 } });
        let res = builder.toSQL();
        if (res.sql === "UPDATE users SET age = age * ? WHERE name = ?") {
            logSuccess("✔ Test 8 - 更新 $mul通过");
            passedTests++;
        } else {
            logFailure("X Test 8 - 更新 $mul失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 8 出错：", e); failedTests++; }

    // Test 10: 更新操作多条件查询
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'Eve' ,role: 'admin'}).update({ $set: { active: false } });
        let res = builder.toSQL();
        if (res.sql === "UPDATE users SET active = ? WHERE name = ? AND role = ?") {
            logSuccess("✔ Test 10 - 多条件更新通过");
            passedTests++;
        } else {

            console.log(res.params)
            logFailure("X Test 10 - 多条件更新失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 10 出错：", e); failedTests++; }

    // ------------------------- 删除测试 -------------------------

    // Test 11: 单条删除（DELETE，LIMIT 1）
    try {
        const builder = new MongoDeleteBuilder('users');
        builder.query({ name: 'John Doe' }).single();
        let res = builder.toSQL();
        if (res.sql === "DELETE FROM users WHERE name = ? LIMIT 1") {
            logSuccess("✔ Test 11 - 单条删除通过");
            passedTests++;
        } else {
            logFailure("X Test 11 - 单条删除失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 11 出错：", e); failedTests++; }

    // Test 12: 多条删除（DELETE，无 LIMIT）
    try {
        const builder = new MongoDeleteBuilder('users');
        builder.query({ active: false });
        let res = builder.toSQL();
        if (res.sql === "DELETE FROM users WHERE active = ?") {
            logSuccess("✔ Test 12 - 多条删除通过");
            passedTests++;
        } else {
            logFailure("X Test 12 - 多条删除失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 12 出错：", e); failedTests++; }

    // ------------------------- 查询测试 -------------------------

    // Test 13: 简单 SELECT 查询（单条件）
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ age: { $gte: 18 } }).project(['name', 'age']);
        let res = builder.toSQL();
        if (res.sql === "SELECT name, age FROM users WHERE age >= ?") {
            logSuccess("✔ Test 13 - 简单查询通过");
            passedTests++;
        } else {
            logFailure("X Test 13 - 简单查询失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 13 出错：", e); failedTests++; }

    // Test 14: 多条件 SELECT 查询（隐式 $and）
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ age: { $gte: 18 }, active: true });
        let res = builder.toSQL();
        if (res.sql === "SELECT * FROM users WHERE age >= ? AND active = ?") {
            logSuccess("✔ Test 14 - 多条件查询通过");
            passedTests++;
        } else {
            logFailure("X Test 14 - 多条件查询失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 14 出错：", e); failedTests++; }

    // Test 15: SELECT 查询使用 $or 操作符
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ $or: [{ age: { $lt: 18 } }, { active: false }] });
        let res = builder.toSQL();
        if (res.sql === "SELECT * FROM users WHERE (age < ? OR active = ?)") {
            logSuccess("✔ Test 15 - $or 查询通过");
            passedTests++;
        } else {
            logFailure("X Test 15 - $or 查询失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 15 出错：", e); failedTests++; }

    // Test 16: SELECT 查询使用 $nor 操作符
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ $nor: [{ age: { $lt: 18 } }, { active: false }] });
        let res = builder.toSQL();
        if (res.sql === "SELECT * FROM users WHERE NOT (age < ? OR active = ?)") {
            logSuccess("✔ Test 16 - $nor 查询通过");
            passedTests++;
        } else {
            logFailure("X Test 16 - $nor 查询失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 16 出错：", e); failedTests++; }

    // Test 17: SELECT 查询使用嵌套条件
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({
            $and: [
                { age: { $gte: 18 } },
                { $or: [{ active: true }, { role: 'admin' }] }
            ]
        });
        let res = builder.toSQL();
        if (res.sql === "SELECT * FROM users WHERE age >= ? AND (active = ? OR role = ?)") {
            logSuccess("✔ Test 17 - 嵌套查询通过");
            passedTests++;
        } else {
            logFailure("X Test 17 - 嵌套查询失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 17 出错：", e); failedTests++; }

    // Test 18: SELECT 查询使用 $in（常规数组）
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ age: { $in: [20, 25, 30] } });
        let res = builder.toSQL();
        if (res.sql === "SELECT * FROM users WHERE age IN (?, ?, ?)") {
            logSuccess("✔ Test 18 - $in 数组查询通过");
            passedTests++;
        } else {
            logFailure("X Test 18 - $in 数组查询失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 18 出错：", e); failedTests++; }

    // Test 19: SELECT 查询使用 $nin 操作符
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ age: { $nin: [20, 25] } });
        let res = builder.toSQL();
        if (res.sql === "SELECT * FROM users WHERE age NOT IN (?, ?)") {
            logSuccess("✔ Test 19 - $nin 查询通过");
            passedTests++;
        } else {
            logFailure("X Test 19 - $nin 查询失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 19 出错：", e); failedTests++; }

    // Test 20: SELECT 查询使用 $all 操作符
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ tags: { $all: ['vip', 'active'] } });
        let res = builder.toSQL();
        // 期望的 SQL 查询应该是：SELECT * FROM users WHERE (JSON_CONTAINS(tags, ?) AND JSON_CONTAINS(tags, ?))
        if (res.sql === "SELECT * FROM users WHERE (JSON_CONTAINS(tags, ?) AND JSON_CONTAINS(tags, ?))") {
            logSuccess("✔ Test 20 - $all 查询通过");
            passedTests++;
        } else {
            logFailure("X Test 20 - $all 查询失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 20 出错：", e); failedTests++; }


    // ------------------------- 聚合查询 -------------------------

    // Test 21: 聚合查询：单分组（GROUP BY）与 SUM
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', totalAmount: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS totalAmount FROM orders WHERE status = ? GROUP BY customer_id") {
            logSuccess("✔ Test 21 - 聚合查询 SUM通过");
            passedTests++;
        } else {
            logFailure("X Test 21 - 聚合查询 SUM失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 21 出错：", e); failedTests++; }

    // Test 22: 聚合查询：单分组与 AVG
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', avgAmount: { $avg: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, AVG(amount) AS avgAmount FROM orders WHERE status = ? GROUP BY customer_id") {
            logSuccess("✔ Test 22 - 聚合查询 AVG通过");
            passedTests++;
        } else {
            logFailure("X Test 22 - 聚合查询 AVG失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 22 出错：", e); failedTests++; }

    // Test 23: 聚合查询：单分组与 MIN
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', minAmount: { $min: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, MIN(amount) AS minAmount FROM orders WHERE status = ? GROUP BY customer_id") {
            logSuccess("✔ Test 23 - 聚合查询 MIN通过");
            passedTests++;
        } else {
            logFailure("X Test 23 - 聚合查询 MIN失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 23 出错：", e); failedTests++; }

    // Test 24: 聚合查询：单分组与 MAX
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', maxAmount: { $max: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, MAX(amount) AS maxAmount FROM orders WHERE status = ? GROUP BY customer_id") {
            logSuccess("✔ Test 24 - 聚合查询 MAX通过");
            passedTests++;
        } else {
            logFailure("X Test 24 - 聚合查询 MAX失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 24 出错：", e); failedTests++; }

    // Test 25: 聚合查询：单分组同时计算 SUM 与 AVG
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' }, avg: { $avg: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total, AVG(amount) AS avg FROM orders WHERE status = ? GROUP BY customer_id") {
            logSuccess("✔ Test 25 - 聚合查询 SUM 和 AVG");
            passedTests++;
        } else {
            logFailure("X Test 25 - 聚合查询 SUM 和 AVG失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 25 出错：", e); failedTests++; }

    // Test 26: 聚合查询：分组后排序
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY customer_id ORDER BY total DESC") {
            logSuccess("✔ Test 26 - 聚合查询排序");
            passedTests++;
        } else {
            logFailure("X Test 26 - 聚合查询排序失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 26 出错：", e); failedTests++; }

    // Test 27: 聚合查询：分组后跳过与限制
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .skip(5)
            .limit(10);
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY customer_id LIMIT 10 OFFSET 5") {
            logSuccess("✔ Test 27 - 聚合查询跳过与限制");
            passedTests++;
        } else {
            logFailure("X Test 27 - 聚合查询跳过与限制失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 27 出错：", e); failedTests++; }

    // Test 28: 聚合查询：分组后跳过与限制（$skip 与 $limit）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .skip(5)
            .limit(10);
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY customer_id LIMIT 10 OFFSET 5") {
            logSuccess("✔ Test 28 - 聚合跳过与限制");
            passedTests++;
        } else {
            logFailure("X Test 28 - 聚合跳过与限制失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 28 出错：", e); failedTests++; }

    // Test 29: 聚合查询：使用 $lookup（JOIN 模拟）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .lookup({ from: 'users', localField: 'user_id', foreignField: 'id', as: 'user' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders LEFT JOIN users AS user ON orders.user_id = user.id WHERE status = ? GROUP BY customer_id") {
            logSuccess("✔ Test 29 - 聚合查询 $lookup通过");
            passedTests++;
        } else {
            logFailure("X Test 29 - 聚合查询 $lookup失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 29 出错：", e); failedTests++; }

    // Test 30: 聚合查询：使用 $unwind 阶段
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .unwind('items')
            .group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders /* UNWIND(items) */ WHERE status = ? GROUP BY customer_id") {
            logSuccess("✔ Test 30 - 聚合查询 $unwind通过");
            passedTests++;
        } else {
            logFailure("X Test 30 - 聚合查询 $unwind失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 30 出错：", e); failedTests++; }

    // Test 31: 聚合查询：多个 $match 阶段累加条件
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .match({ region: 'US' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders WHERE status = ? AND region = ? GROUP BY customer_id") {
            logSuccess("✔ Test 31 - 多重 $match通过");
            passedTests++;
        } else {
            logFailure("X Test 31 - 多重 $match失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 31 出错：", e); failedTests++; }

    // Test 32: 聚合查询：使用 $project 阶段重新构造 SELECT
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.project(['field1','field2'])
            .match({ status: 'completed' });
        let res = agg.toSQL();
        if (res.sql === "SELECT field1, field2 FROM orders WHERE status = ?") {
            logSuccess("✔ Test 32 - 聚合查询 $project通过");
            passedTests++;
        } else {
            logFailure("X Test 32 - 聚合查询 $project失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 32 出错：", e); failedTests++; }

    // Test 33: 聚合查询：仅使用 $limit 阶段
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .limit(5);
        let res = agg.toSQL();
        if (res.sql === "SELECT * FROM orders WHERE status = ? LIMIT 5") {
            logSuccess("✔ Test 33 - 聚合查询仅 $limit通过");
            passedTests++;
        } else {
            logFailure("X Test 33 - 聚合查询仅 $limit失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 33 出错：", e); failedTests++; }

    // Test 34: 聚合查询：仅使用 $skip 阶段（自动补全 LIMIT）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .skip(3);
        let res = agg.toSQL();
        if (res.sql === "SELECT * FROM orders WHERE status = ? LIMIT 18446744073709551615 OFFSET 3") {
            logSuccess("✔ Test 34 - 聚合查询仅 $skip通过");
            passedTests++;
        } else {
            logFailure("X Test 34 - 聚合查询仅 $skip失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 34 出错：", e); failedTests++; }

    // Test 35: 聚合查询：无 match 条件，仅 group
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders GROUP BY customer_id") {
            logSuccess("✔ Test 35 - 聚合查询无 match通过");
            passedTests++;
        } else {
            logFailure("X Test 35 - 聚合查询无 match失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 35 出错：", e); failedTests++; }

    // Test 36: 聚合查询：group _id 为常量
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: 'all', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT 'all' AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY _id") {
            logSuccess("✔ Test 36 - 聚合查询常量 _id通过");
            passedTests++;
        } else {
            logFailure("X Test 36 - 聚合查询常量 _id失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 36 出错：", e); failedTests++; }

    // Test 37: 聚合查询：复杂 group（多个计算字段）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({
                _id: '$customer_id',
                total: { $sum: '$amount' },
                avg: { $avg: '$amount' },
                min: { $min: '$amount' },
                max: { $max: '$amount' }
            });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total, AVG(amount) AS avg, MIN(amount) AS min, MAX(amount) AS max FROM orders WHERE status = ? GROUP BY customer_id") {
            logSuccess("✔ Test 37 - 复杂 group 聚合通过");
            passedTests++;
        } else {
            logFailure("X Test 37 - 复杂 group 聚合失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 37 出错：", e); failedTests++; }

    // Test 38: 聚合查询：使用多重排序
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1, _id: 1 });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY customer_id ORDER BY total DESC, _id ASC") {
            logSuccess("✔ Test 38 - 聚合多重排序通过");
            passedTests++;
        } else {
            logFailure("X Test 38 - 聚合多重排序失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 38 出错：", e); failedTests++; }

    // Test 39: 聚合查询：结合所有阶段（match, group, sort, skip, limit）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 })
            .skip(2)
            .limit(5);
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY customer_id ORDER BY total DESC LIMIT 5 OFFSET 2") {
            logSuccess("✔ Test 39 - 完整聚合管道通过");
            passedTests++;
        } else {
            logFailure("X Test 39 - 完整聚合管道失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 39 出错：", e); failedTests++; }

    // Test 40: 聚合查询：无匹配条件，仅 group 与 sort
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: 1 });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders GROUP BY customer_id ORDER BY total ASC") {
            logSuccess("✔ Test 40 - 聚合无匹配条件通过");
            passedTests++;
        } else {
            logFailure("X Test 40 - 聚合无匹配条件失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 40 出错：", e); failedTests++; }

    // Test 41: 聚合查询：group _id 为 null（所有记录归为一组）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: null, total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT SUM(amount) AS total FROM orders WHERE status = ?") {
            logSuccess("✔ Test 41 - 聚合 group _id 为 null");
            passedTests++;
        } else {
            logFailure("X Test 41 - 聚合 group _id 为 null失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 41 出错：", e); failedTests++; }

    // Test 42: 聚合查询：group 使用字符串 _id（非字段）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: 'all', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT 'all' AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY _id") {
            logSuccess("✔ Test 42 - 聚合 group _id 为字符串");
            passedTests++;
        } else {
            logFailure("X Test 42 - 聚合 group _id 为字符串失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 42 出错：", e); failedTests++; }

    // Test 43: 聚合查询：复杂管道（match, group, sort, skip, limit, lookup）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .lookup({ from: 'users', localField: 'user_id', foreignField: 'id', as: 'u' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 })
            .skip(1)
            .limit(5);
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders LEFT JOIN users AS u ON orders.user_id = u.id WHERE status = ? GROUP BY customer_id ORDER BY total DESC LIMIT 5 OFFSET 1") {
            logSuccess("✔ Test 43 - 复杂聚合管道通过");
            passedTests++;
        } else {
            logFailure("X Test 43 - 复杂聚合管道失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 43 出错：", e); failedTests++; }

    // Test 44: 聚合查询：group _id 为常量
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: 'all', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT 'all' AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY _id") {
            logSuccess("✔ Test 44 - 聚合 group _id 为常量通过");
            passedTests++;
        } else {
            logFailure("X Test 44 - 聚合 group _id 为常量失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 44 出错：", e); failedTests++; }

    // Test 45: 聚合查询：复杂 group _id（包含多个字段）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: { customer: '$customer_id', date: '$order_date' }, total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id, order_date, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY customer_id, order_date") {
            logSuccess("✔ Test 45 - 复杂 group _id通过");
            passedTests++;
        } else {
            logFailure("X Test 45 - 复杂 group _id失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 45 出错：", e); failedTests++; }

    // Test 46: 聚合查询：group 结果排序后再限制数量
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 })
            .limit(3);
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY customer_id ORDER BY total DESC LIMIT 3") {
            logSuccess("✔ Test 46 - 聚合排序后 LIMIT通过");
            passedTests++;
        } else {
            logFailure("X Test 46 - 聚合排序后 LIMIT失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 46 出错：", e); failedTests++; }

    // Test 47: 聚合查询：使用多个 $sort 键排序
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1, _id: 1 });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders WHERE status = ? GROUP BY customer_id ORDER BY total DESC, _id ASC") {
            logSuccess("✔ Test 47 - 聚合查询多键排序通过");
            passedTests++;
        } else {
            logFailure("X Test 47 - 聚合查询多键排序失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 47 出错：", e); failedTests++; }

    // Test 48: 聚合查询：复杂管道（match, group, sort, skip, limit, lookup）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed', region: 'US' })
            .lookup({ from: 'users', localField: 'user_id', foreignField: 'id', as: 'u' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 })
            .skip(1)
            .limit(5);
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders LEFT JOIN users AS u ON orders.user_id = u.id WHERE status = ? AND region = ? GROUP BY customer_id ORDER BY total DESC LIMIT 5 OFFSET 1") {
            logSuccess("✔ Test 48 - 复杂聚合管道通过");
            passedTests++;
        } else {
            logFailure("X Test 48 - 复杂聚合管道失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 48 出错：", e); failedTests++; }

    // Test 49: 聚合查询：仅使用 group 阶段（无 match、无 sort）
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        if (res.sql === "SELECT customer_id AS _id, SUM(amount) AS total FROM orders GROUP BY customer_id") {
            logSuccess("✔ Test 49 - 聚合查询仅 group 阶段通过");
            passedTests++;
        } else {
            logFailure("X Test 49 - 聚合查询仅 group 阶段失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 49 出错：", e); failedTests++; }

    // Test 50: 聚合查询：综合测试—多阶段、多函数、排序、限制、跳过
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed', region: 'EU' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' }, avg: { $avg: '$amount' } })
            .project(["extra"])
            .sort({ total: -1 })
            .skip(2)
            .limit(4);
        let res = agg.toSQL();
        if (res.sql === "SELECT extra, customer_id AS _id, SUM(amount) AS total, AVG(amount) AS avg FROM orders WHERE status = ? AND region = ? GROUP BY customer_id ORDER BY total DESC LIMIT 4 OFFSET 2") {
            logSuccess("✔ Test 50 - 综合聚合管道通过");
            passedTests++;
        } else {
            logFailure("X Test 50 - 综合聚合管道失败", res.sql);
            failedTests++;
        }
    } catch (e) { logFailure("X Test 50 出错：", e); failedTests++; }

    // ------------------------- 测试结果 -------------------------
    console.log(`\n测试结果总结:`);
    console.log(`通过的测试: ${passedTests}`);
    console.log(`失败的测试: ${failedTests}`);
})();

