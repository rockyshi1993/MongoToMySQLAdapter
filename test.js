"use strict";

// ====================================================
// 引入转换模块中导出的所有接口
// 注意：确保 mongo2mysql.js 与 test.js 位于同一目录下
// ====================================================
const {
    MongoQueryBuilder,
    MongoAggregationBuilder,
    MongoUpdateBuilder,
    MongoDeleteBuilder,
    MongoInsertBuilder,
    SubQuery,
    mongoToMySQL,
    mongoToMySQLWithJoinsOptimized
} = require('./index');

// ====================================================
// 使用立即执行的异步函数封装测试用例，确保执行顺序
// ====================================================
(async () => {

    // ----------------------------------------------------
    // INSERT 测试用例
    // ----------------------------------------------------

    // Test 1: 单条插入（INSERT ONE）
    // 目的：验证单条记录插入时生成的 SQL 语句和参数列表是否正确。
    try {
        const builder = new MongoInsertBuilder('users');
        builder.insertOne({ name: 'John Doe', age: 30 });
        let res = builder.toSQL();
        console.log("Test 1 - 单条插入\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 1 出错：", e); }

    // Test 2: 批量插入（INSERT MANY）
    // 目的：验证批量插入生成的 SQL 是否正确，并且参数顺序符合预期。
    try {
        const builder = new MongoInsertBuilder('users');
        builder.insertMany([
            { name: 'Alice', age: 25 },
            { name: 'Bob', age: 28 }
        ]);
        let res = builder.toSQL();
        console.log("Test 2 - 批量插入\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 2 出错：", e); }

    // Test 3: Upsert 插入（INSERT + ON DUPLICATE KEY UPDATE）
    // 目的：验证开启 upsert 后生成的 SQL 包含 ON DUPLICATE KEY UPDATE 子句。
    try {
        const builder = new MongoInsertBuilder('users');
        builder.insertOne({ id: 1, name: 'Charlie', age: 40 }).upsert(true, ['name', 'age']);
        let res = builder.toSQL();
        console.log("Test 3 - Upsert 插入\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 3 出错：", e); }

    // Test 4: 插入测试：字段顺序验证
    // 目的：验证当对象属性顺序不固定时生成的 SQL 与参数能正确对应。
    try {
        const builder = new MongoInsertBuilder('users');
        builder.insertOne({ age: 22, name: 'David' });
        let res = builder.toSQL();
        console.log("Test 4 - 字段顺序插入\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 4 出错：", e); }

    // ----------------------------------------------------
    // UPDATE 测试用例
    // ----------------------------------------------------

    // Test 5: 单条更新 $set
    // 目的：验证使用 $set 更新单条记录生成的 SQL 和参数是否正确。
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'John Doe' }).update({ $set: { age: 31 } });
        let res = builder.toSQL();
        console.log("Test 5 - 单条更新 $set\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 5 出错：", e); }

    // Test 6: 更新操作 $inc
    // 目的：验证使用 $inc 操作符生成的 SQL 是否正确构造加法运算。
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'Alice' }).update({ $inc: { age: 1 } });
        let res = builder.toSQL();
        console.log("Test 6 - 更新 $inc\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 6 出错：", e); }

    // Test 7: 更新操作 $unset
    // 目的：验证使用 $unset 时 SQL 将字段置为 NULL。
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'Bob' }).update({ $unset: { age: "" } });
        let res = builder.toSQL();
        console.log("Test 7 - 更新 $unset\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 7 出错：", e); }

    // Test 8: 更新操作 $mul
    // 目的：验证使用 $mul 生成的 SQL 是否正确构造乘法运算。
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'Alice' }).update({ $mul: { age: 2 } });
        let res = builder.toSQL();
        console.log("Test 8 - 更新 $mul\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 8 出错：", e); }

    // Test 9: 批量更新操作
    // 目的：验证批量更新时生成的 CASE WHEN 语法是否正确，以及参数合并顺序。
    try {
        const builder = new MongoUpdateBuilder('users', 'id');
        builder.query({ active: true }).update([
            { id: 1, age: 35 },
            { id: 2, age: 45 }
        ]);
        let res = builder.toSQL();
        console.log("Test 9 - 批量更新\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 9 出错：", e); }

    // Test 10: 更新操作多条件查询
    // 目的：验证当使用多个 query() 调用时，生成的 WHERE 子句是否包含所有条件。
    try {
        const builder = new MongoUpdateBuilder('users');
        builder.query({ name: 'Eve' }).query({ role: 'admin' }).update({ $set: { active: false } });
        let res = builder.toSQL();
        console.log("Test 10 - 多条件更新\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 10 出错：", e); }

    // ----------------------------------------------------
    // DELETE 测试用例
    // ----------------------------------------------------

    // Test 11: 单条删除（DELETE，LIMIT 1）
    // 目的：验证删除操作中使用 single() 时生成的 SQL 是否包含 LIMIT 1。
    try {
        const builder = new MongoDeleteBuilder('users');
        builder.query({ name: 'John Doe' }).single();
        let res = builder.toSQL();
        console.log("Test 11 - 单条删除\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 11 出错：", e); }

    // Test 12: 多条删除（DELETE，无 LIMIT）
    // 目的：验证不调用 single() 时生成的 DELETE 语句不包含 LIMIT 子句。
    try {
        const builder = new MongoDeleteBuilder('users');
        builder.query({ active: false });
        let res = builder.toSQL();
        console.log("Test 12 - 多条删除\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 12 出错：", e); }

    // ----------------------------------------------------
    // SELECT 与 分页 测试用例
    // ----------------------------------------------------

    // Test 13: 简单 SELECT 查询（单条件）
    // 目的：验证简单查询生成的 SELECT 与 WHERE 子句及投影（project）是否正确。
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ age: { $gte: 18 } }).project(['name', 'age']);
        let res = builder.toSQL();
        console.log("Test 13 - 简单查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 13 出错：", e); }

    // Test 14: 多条件 SELECT 查询（隐式 $and）
    // 目的：验证连续调用 query() 时生成的 WHERE 子句是否正确拼接为 AND。
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ age: { $gte: 18 } }).query({ active: true });
        let res = builder.toSQL();
        console.log("Test 14 - 多条件查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 14 出错：", e); }

    // Test 15: SELECT 查询使用 $or 操作符
    // 目的：验证使用 $or 时生成的 SQL 是否正确构造 OR 子句。
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ $or: [{ age: { $lt: 18 } }, { active: false }] });
        let res = builder.toSQL();
        console.log("Test 15 - $or 查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 15 出错：", e); }

    // Test 16: SELECT 查询使用 $nor 操作符
    // 目的：验证使用 $nor 时生成的 SQL 是否正确构造 NOT (...) 结构。
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ $nor: [{ age: { $lt: 18 } }, { active: false }] });
        let res = builder.toSQL();
        console.log("Test 16 - $nor 查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 16 出错：", e); }

    // Test 17: SELECT 查询使用嵌套条件
    // 目的：验证多层嵌套条件生成正确的括号结构。
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({
            $and: [
                { age: { $gte: 18 } },
                { $or: [{ active: true }, { role: 'admin' }] }
            ]
        });
        let res = builder.toSQL();
        console.log("Test 17 - 嵌套查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 17 出错：", e); }

    // Test 18: SELECT 查询使用 $in（常规数组）
    // 目的：验证 $in 操作符生成的 SQL 中 IN 子句及参数列表是否正确。
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ age: { $in: [20, 25, 30] } });
        let res = builder.toSQL();
        console.log("Test 18 - $in 数组查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 18 出错：", e); }

    // Test 19: SELECT 查询使用 $nin 操作符
    // 目的：验证 $nin 操作符生成的 SQL 是否正确构造 NOT IN 子句。
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ age: { $nin: [20, 25] } });
        let res = builder.toSQL();
        console.log("Test 19 - $nin 查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 19 出错：", e); }

    // Test 20: SELECT 查询使用 $all 操作符
    // 目的：验证 $all 操作符生成的 SQL 是否使用 JSON_CONTAINS 处理数组包含关系。
    try {
        const builder = new MongoQueryBuilder('users');
        builder.query({ tags: { $all: ['vip', 'active'] } });
        let res = builder.toSQL();
        console.log("Test 20 - $all 查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 20 出错：", e); }

    // Test 21: 分页查询（独立转换函数 mongoToMySQL）
    // 目的：验证生成的分页查询 SQL 包含 ORDER BY 和 LIMIT 子句。
    try {
        const query = { age: { $gte: 18 } };
        const table = 'users';
        let res = mongoToMySQL(query, table, 10, 'age DESC');
        console.log("Test 21 - 分页查询\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 21 出错：", e); }

    // ====================================================
    // 聚合查询测试用例（至少 25 个场景）
    // ====================================================

    // Test 22: 聚合查询：单分组（GROUP BY）与 SUM
    // 目的：验证聚合查询中使用 $sum 生成的 SQL 是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', totalAmount: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 22 - 聚合查询 SUM\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 22 出错：", e); }

    // Test 23: 聚合查询：单分组与 AVG
    // 目的：验证聚合查询中使用 $avg 生成的 SQL 是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', avgAmount: { $avg: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 23 - 聚合查询 AVG\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 23 出错：", e); }

    // Test 24: 聚合查询：单分组与 MIN
    // 目的：验证聚合查询中使用 $min 生成的 SQL 是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', minAmount: { $min: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 24 - 聚合查询 MIN\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 24 出错：", e); }

    // Test 25: 聚合查询：单分组与 MAX
    // 目的：验证聚合查询中使用 $max 生成的 SQL 是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', maxAmount: { $max: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 25 - 聚合查询 MAX\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 25 出错：", e); }

    // Test 26: 聚合查询：单分组同时计算 SUM 与 AVG
    // 目的：验证在同一 group 中计算多个聚合函数时 SQL 是否正确生成。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' }, avg: { $avg: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 26 - 聚合查询 SUM 与 AVG\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 26 出错：", e); }

    // Test 27: 聚合查询：多分组（_id 为对象）
    // 目的：验证 _id 为对象时生成的 GROUP BY 子句是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: { customer: '$customer_id', date: '$order_date' }, total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 27 - 多分组聚合\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 27 出错：", e); }

    // Test 28: 聚合查询：分组后排序
    // 目的：验证在聚合查询中添加 $sort 阶段时 SQL 是否正确追加 ORDER BY 子句。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 });
        let res = agg.toSQL();
        console.log("Test 28 - 聚合排序\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 28 出错：", e); }

    // Test 29: 聚合查询：分组后跳过与限制（$skip 与 $limit）
    // 目的：验证在聚合查询中同时使用 $skip 与 $limit 时生成的 SQL 是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .skip(5)
            .limit(10);
        let res = agg.toSQL();
        console.log("Test 29 - 聚合跳过与限制\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 29 出错：", e); }

    // Test 30: 聚合查询：使用 $lookup（JOIN 模拟）
    // 目的：验证在聚合查询中添加 $lookup 阶段时生成的 JOIN 子句是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .lookup({ from: 'users', localField: 'user_id', foreignField: 'id', as: 'user' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 30 - 聚合查询 $lookup\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 30 出错：", e); }

    // Test 31: 聚合查询：使用 $unwind 阶段
    // 目的：验证在聚合查询中添加 $unwind 阶段时生成的 SQL 是否正确附加注释（目前仅作为注释输出）。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .unwind('items')
            .group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 31 - 聚合查询 $unwind\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 31 出错：", e); }

    // Test 32: 聚合查询：多个 $match 阶段累加条件
    // 目的：验证多次调用 match() 时生成的 WHERE 子句条件是否累计。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .match({ region: 'US' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 32 - 多重 $match\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 32 出错：", e); }

    // Test 33: 聚合查询：使用 $project 阶段重新构造 SELECT
    // 目的：验证 $project 阶段能正确覆盖默认 SELECT 子句。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.project({ field1: 1, field2: 1 })
            .match({ status: 'completed' });
        let res = agg.toSQL();
        console.log("Test 33 - 聚合查询 $project\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 33 出错：", e); }

    // Test 34: 聚合查询：仅使用 $limit 阶段
    // 目的：验证聚合查询中只有 $limit 时生成正确的 LIMIT 子句。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .limit(5);
        let res = agg.toSQL();
        console.log("Test 34 - 聚合查询仅 $limit\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 34 出错：", e); }

    // Test 35: 聚合查询：仅使用 $skip 阶段（自动补全 LIMIT）
    // 目的：验证当只调用 $skip 时，系统自动添加极大 LIMIT 的处理。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .skip(3);
        let res = agg.toSQL();
        console.log("Test 35 - 聚合查询仅 $skip\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 35 出错：", e); }

    // Test 36: 聚合查询：无 match 条件，仅 group
    // 目的：验证无 match 条件时是否生成默认 WHERE 子句。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 36 - 聚合查询无 match\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 36 出错：", e); }

    // Test 37: 聚合查询：group _id 为常量
    // 目的：验证当 _id 为常量时，SQL 生成是否符合预期。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: 'all', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 37 - 聚合查询常量 _id\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 37 出错：", e); }

    // Test 38: 聚合查询：复杂 group（多个计算字段）
    // 目的：验证 group 阶段中多个计算字段（SUM, AVG, MIN, MAX）同时生成正确。
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
        console.log("Test 38 - 复杂 group 聚合\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 38 出错：", e); }

    // Test 39: 聚合查询：使用多重排序
    // 目的：验证 $sort 阶段中指定多个排序键时 SQL 是否正确生成。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1, _id: 1 });
        let res = agg.toSQL();
        console.log("Test 39 - 聚合多重排序\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 39 出错：", e); }

    // Test 40: 聚合查询：结合所有阶段（match, group, sort, skip, limit）
    // 目的：验证完整聚合管道生成的 SQL 是否正确组合各阶段。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 })
            .skip(2)
            .limit(5);
        let res = agg.toSQL();
        console.log("Test 40 - 完整聚合管道\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 40 出错：", e); }

    // Test 41: 聚合查询：无匹配条件，仅 group 与 sort
    // 目的：验证当没有 match 条件时生成默认 WHERE 子句以及 group 和 sort 是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: 1 });
        let res = agg.toSQL();
        console.log("Test 41 - 聚合无 match 条件\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 41 出错：", e); }

    // Test 42: 聚合查询：group _id 为 null（所有记录归为一组）
    // 目的：验证当 _id 为 null 时，所有记录是否归为一组。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: null, total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 42 - 聚合 group _id 为 null\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 42 出错：", e); }

    // Test 43: 聚合查询：group 使用字符串 _id（非字段）
    // 目的：验证当 _id 为普通字符串时生成的 SQL 是否正确处理。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: 'all', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 43 - 聚合 group _id 为字符串\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 43 出错：", e); }

    // Test 44: 聚合查询：group 与 $avg 和 $min 同时使用
    // 目的：验证同一 group 中同时使用 $avg 与 $min 时生成的 SQL 是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', avgAmount: { $avg: '$amount' }, minAmount: { $min: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 44 - 聚合同时使用 AVG 与 MIN\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 44 出错：", e); }

    // Test 45: 聚合查询：复杂 group _id（包含多个字段）
    // 目的：验证 group _id 为对象，包含多个字段时 SQL 是否正确生成。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: { customer: '$customer_id', date: '$order_date' }, total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 45 - 复杂 group _id\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 45 出错：", e); }

    // Test 46: 聚合查询：group 结果排序后再限制数量
    // 目的：验证在 group 后再进行排序与 LIMIT 时 SQL 是否正确组合。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 })
            .limit(3);
        let res = agg.toSQL();
        console.log("Test 46 - 聚合排序后 LIMIT\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 46 出错：", e); }

    // Test 47: 聚合查询：使用多个 $sort 键排序
    // 目的：验证 $sort 阶段中使用多个排序键时生成的 ORDER BY 子句格式正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1, _id: 1 });
        let res = agg.toSQL();
        console.log("Test 47 - 多键排序聚合\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 47 出错：", e); }

    // Test 48: 聚合查询：复杂管道（match, group, sort, skip, limit, lookup）
    // 目的：验证完整聚合管道组合各阶段时生成的 SQL 是否正确组合。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed', region: 'US' })
            .lookup({ from: 'users', localField: 'user_id', foreignField: 'id', as: 'u' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' } })
            .sort({ total: -1 })
            .skip(1)
            .limit(5);
        let res = agg.toSQL();
        console.log("Test 48 - 复杂聚合管道\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 48 出错：", e); }

    // Test 49: 聚合查询：仅使用 group 阶段（无 match、无 sort）
    // 目的：验证仅有 group 阶段时生成的 SQL 是否仍然有效。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.group({ _id: '$customer_id', total: { $sum: '$amount' } });
        let res = agg.toSQL();
        console.log("Test 49 - 仅 group 阶段聚合\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 49 出错：", e); }

    // Test 50: 聚合查询：综合测试—多阶段、多函数、排序、限制、跳过
    // 目的：验证一个综合聚合管道（包含 match、group、project、sort、skip、limit）生成的 SQL 是否正确。
    try {
        const agg = new MongoAggregationBuilder('orders');
        agg.match({ status: 'completed', region: 'EU' })
            .group({ _id: '$customer_id', total: { $sum: '$amount' }, avg: { $avg: '$amount' } })
            .project({ extra: 1 })
            .sort({ total: -1 })
            .skip(2)
            .limit(4);
        let res = agg.toSQL();
        console.log("Test 50 - 综合聚合管道\nSQL:", res.sql, "\nParams:", res.params);
    } catch (e) { console.error("Test 50 出错：", e); }

})();
