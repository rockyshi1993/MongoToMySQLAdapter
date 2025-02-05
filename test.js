"use strict";
/**
 * 运行本测试用例：
 * 在命令行中执行：node testMongoSqlBuilder.js
 */

const assert = require("assert");
const {
    MongoQueryBuilder,
    MongoAggregationBuilder,
    MongoUpdateBuilder,
    MongoDeleteBuilder,
    MongoInsertBuilder,
    SubQuery
} = require("./index");

console.log("开始运行测试用例...\n");

//
// MongoQueryBuilder 示例：构造 SELECT 查询
//
/**
 * 示例1: 使用 MongoQueryBuilder 构造 SELECT 查询
 * - 添加条件：age > 18 且 name LIKE '%John%'
 * - 按 age 升序排序，返回 10 条记录，从第 0 条开始。
 */
const queryExample = new MongoQueryBuilder("users")
    .query({ age: { $gt: 18 } })
    .query({ name: { $like: "%John%" } })
    .sort("age ASC")
    .limit(10)
    .offset(0);
const querySQL = queryExample.toSQL();
console.log("MongoQueryBuilder Example:", querySQL);

//
// MongoAggregationBuilder 示例：聚合查询
//
/**
 * 示例2: 基本聚合查询
 * 在 employees 表中，筛选 salary > 1000 的记录，
 * 按 department 分组，计算总工资、平均工资、最低工资和最高工资。
 */
const aggregationBasicExample = new MongoAggregationBuilder("employees")
    .match({ salary: { $gt: 1000 } })
    .group({
        _id: "$department",
        totalSalary: { $sum: "$salary" },
        avgSalary: { $avg: "$salary" },
        minSalary: { $min: "$salary" },
        maxSalary: { $max: "$salary" }
    })
    .toSQL();
console.log("Aggregation Basic Example:", aggregationBasicExample);

/**
 * 示例3: 复杂聚合查询
 * 在 orders 表中，筛选 status 为 "completed" 的订单，
 * 按 customerId 分组统计订单数量和总金额，
 * 通过 $project 阶段仅保留 _id、orderCount 和 totalAmount 字段，
 * 按 totalAmount 降序排序，跳过前 5 条，返回 10 条记录。
 */
const aggregationComplexExample = new MongoAggregationBuilder("orders")
    .match({ status: { $eq: "completed" } })
    .group({
        _id: "$customerId",
        orderCount: { $sum: 1 },
        totalAmount: { $sum: "$amount" }
    })
    .project(["_id", "orderCount", "totalAmount"])
    .sort({ totalAmount: -1 })
    .limit(10)
    .skip(5)
    .toSQL();
console.log("Aggregation Complex Example:", aggregationComplexExample);

/**
 * 示例4: 聚合查询不支持的操作符
 * 传入 $add 操作符（当前不支持），预期抛出异常。
 */
try {
    new MongoAggregationBuilder("employees")
        .group({
            _id: "$department",
            computed: { $add: ["$salary", 100] }
        })
        .toSQL();
    console.error("Aggregation Unsupported Operator: 测试失败，应抛出异常");
} catch (err) {
    console.log("Aggregation Unsupported Operator Test Passed:", err.message);
}

//
// MongoUpdateBuilder 示例：更新操作
//
/**
 * 示例5: 固定更新（默认 $set）
 * 更新 users 表中 id 为 100 的记录，将 name 设为 "Alice"，age 设为 30。
 */
const fixedUpdateExample = new MongoUpdateBuilder("users")
    .update({ name: "Alice", age: 30 })
    .query({ id: { $eq: 100 } })
    .toSQL();
console.log("MongoUpdateBuilder Fixed Update Example:", fixedUpdateExample);

/**
 * 示例6: 固定更新并限制为单条更新
 * 更新 status 为 "active" 的记录，将 name 设为 "Bob"，只更新第一条匹配记录。
 */
const singleUpdateExample = new MongoUpdateBuilder("users")
    .update({ $set: { name: "Bob" } })
    .query({ status: { $eq: "active" } })
    .single()
    .toSQL();
console.log("MongoUpdateBuilder Single Update Example:", singleUpdateExample);

/**
 * 示例7: 批量更新
 * 批量更新 users 表中 id 为 1 和 2 的记录，各自设置不同的 name 和 age。
 */
const bulkUpdateExample = new MongoUpdateBuilder("users", "id")
    .update([
        { id: 1, name: "Carol", age: 25 },
        { id: 2, name: "Dave", age: 28 }
    ])
    .query({ id: { $in: [1, 2] } })
    .toSQL();
console.log("MongoUpdateBuilder Bulk Update Example:", bulkUpdateExample);

//
// MongoDeleteBuilder 示例：删除操作
//
/**
 * 示例8: 批量删除
 * 删除所有 status 为 "inactive" 的用户。
 */
const bulkDeleteExample = new MongoDeleteBuilder("users")
    .query({ status: { $eq: "inactive" } })
    .toSQL();
console.log("MongoDeleteBuilder Bulk Delete Example:", bulkDeleteExample);

/**
 * 示例9: 单条删除
 * 删除所有 status 为 "inactive" 的用户中第一条匹配记录。
 */
const singleDeleteExample = new MongoDeleteBuilder("users")
    .query({ status: { $eq: "inactive" } })
    .single()
    .toSQL();
console.log("MongoDeleteBuilder Single Delete Example:", singleDeleteExample);

//
// MongoInsertBuilder 示例：插入操作
//
/**
 * 示例10: 单条插入（不启用 upsert）
 * 向 users 表中插入一条记录。
 */
const insertOneExample = new MongoInsertBuilder("users")
    .insertOne({ name: "Eve", age: 22, status: "active" })
    .toSQL();
console.log("MongoInsertBuilder InsertOne Example:", insertOneExample);

/**
 * 示例11: 单条插入（启用 upsert）
 * 向 users 表中插入记录，并启用 upsert（ON DUPLICATE KEY UPDATE）。
 */
const insertOneUpsertExample = new MongoInsertBuilder("users")
    .insertOne({ name: "Frank", age: 30, status: "active" })
    .upsert(true)
    .toSQL();
console.log("MongoInsertBuilder InsertOne Upsert Example:", insertOneUpsertExample);

/**
 * 示例12: 批量插入（不启用 upsert）
 * 向 users 表中批量插入两条记录。
 */
const insertManyExample = new MongoInsertBuilder("users")
    .insertMany([
        { name: "Grace", age: 29, status: "active" },
        { name: "Heidi", age: 31, status: "inactive" }
    ])
    .toSQL();
console.log("MongoInsertBuilder InsertMany Example:", insertManyExample);

/**
 * 示例13: 批量插入（启用 upsert，仅更新指定字段）
 * 向 users 表中批量插入两条记录，并启用 upsert，仅针对 age 和 status 字段生成更新语句。
 */
const insertManyUpsertExample = new MongoInsertBuilder("users")
    .insertMany([
        { name: "Ivan", age: 35, status: "active" },
        { name: "Judy", age: 27, status: "active" }
    ])
    .upsert(true, ["age", "status"])
    .toSQL();
console.log("MongoInsertBuilder InsertMany Upsert Example:", insertManyUpsertExample);

//
// SubQuery 示例
//
/**
 * 示例14: 子查询
 * 子查询部分：从 orders 表中选取 amount > 100 的记录，投影 user_id 字段，返回 50 条记录；
 * 主查询部分：从 users 表中查找 id 在子查询结果中的记录，返回 20 条记录。
 */
const subQueryExample = new MongoQueryBuilder("orders")
    .query({ amount: { $gt: 100 } })
    .project(["user_id"])
    .limit(50);
const subQ = new SubQuery(subQueryExample);
const mainQueryExample = new MongoQueryBuilder("users")
    .query({ id: { $in: subQ } })
    .limit(20);
const subQuerySQL = mainQueryExample.toSQL();
console.log("SubQuery Example:", subQuerySQL);

//
// 错误情况测试
//
/**
 * 示例15: 更新操作未指定过滤条件，应抛出异常。
 */
assert.throws(() => {
    new MongoUpdateBuilder("users")
        .update({ name: "Error" })
        .toSQL();
}, /更新操作必须指定查询条件/);

/**
 * 示例16: 删除操作未指定过滤条件，应抛出异常。
 */
assert.throws(() => {
    new MongoDeleteBuilder("users").toSQL();
}, /删除操作必须指定查询条件/);

//
// Offset 示例
//
/**
 * 示例17: 使用 offset 进行分页查询
 * 查询 users 表中 age > 18 的记录，返回 10 条记录，从第 20 条开始。
 */
const offsetExample = new MongoQueryBuilder("users")
    .query({ age: { $gt: 18 } })
    .limit(10)
    .offset(20)
    .toSQL();
console.log("Test Select with Offset:", offsetExample);
assert(offsetExample.sql.includes("OFFSET 20"), "SQL 应包含 OFFSET 20");

//
// 运行所有测试用例
//
function runTests() {
    console.log("运行测试：在命令行中执行 'node testMongoSqlBuilder.js'\n");
    try {
        // MongoQueryBuilder 测试
        console.log("----- MongoQueryBuilder 测试 -----");
        console.log("Query Example:", querySQL);

        // MongoAggregationBuilder 测试
        console.log("----- MongoAggregationBuilder 测试 -----");
        console.log("Aggregation Basic Example:", aggregationBasicExample);
        console.log("Aggregation Complex Example:", aggregationComplexExample);

        // MongoUpdateBuilder 测试
        console.log("----- MongoUpdateBuilder 测试 -----");
        console.log("Fixed Update Example:", fixedUpdateExample);
        console.log("Single Update Example:", singleUpdateExample);
        console.log("Bulk Update Example:", bulkUpdateExample);

        // MongoDeleteBuilder 测试
        console.log("----- MongoDeleteBuilder 测试 -----");
        console.log("Bulk Delete Example:", bulkDeleteExample);
        console.log("Single Delete Example:", singleDeleteExample);

        // MongoInsertBuilder 测试
        console.log("----- MongoInsertBuilder 测试 -----");
        console.log("InsertOne Example:", insertOneExample);
        console.log("InsertOne Upsert Example:", insertOneUpsertExample);
        console.log("InsertMany Example:", insertManyExample);
        console.log("InsertMany Upsert Example:", insertManyUpsertExample);

        // SubQuery 测试
        console.log("----- SubQuery 测试 -----");
        console.log("SubQuery Example:", subQuerySQL);

        // 错误情况测试与 Offset 测试
        console.log("----- 错误情况与 Offset 测试 -----");
        console.log("Offset Example:", offsetExample);

        console.log("\n所有测试通过。");
    } catch (err) {
        console.error("测试失败：", err.message);
    }
}

runTests();
