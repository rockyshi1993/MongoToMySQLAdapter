"use strict";

/**
 * 聚合 & 连表查询综合测试文件
 *
 * 包含：
 * 1. 基础聚合（testBasicAggregation）
 * 2. 多次 $lookup 聚合（testMultipleLookups）
 * 3. 仅使用 $match + $project（testProjectAndMatch）
 * 4. $unwind 示例（testUnwindExample）
 * 5. 普通连表查询（非聚合）（testNormalJoins）
 * 6. 统计某一天开始后面日期的用户登录情况（testUserLoginStats）
 * 7. 更加复杂的聚合场景（testComplexAggregationScenario）
 * 8. 新增：登录-下单-复购漏斗测试（testLoginPurchaseFunnel）
 * 9. 新增：更多统计/计算类聚合场景：
 *    - testAveragePurchaseValue
 *    - testMinMaxUserActivity
 */

const assert = require("assert");
const {
    MongoAggregationBuilder,
    MongoQueryBuilder,
    Logger
} = require("../lib"); // 根据你的项目结构调整 import 路径

/* ============================================================
   测试 1：基础聚合（match + group + sort + limit）
============================================================ */
function testBasicAggregation() {
    Logger.info("测试 1：基础聚合");

    const aggBuilder = new MongoAggregationBuilder("sales");
    // match：status = 'completed'
    aggBuilder.match({ status: "completed" });
    // group：按 region 分组，计算 totalSales, orderCount
    aggBuilder.group({
        _id: "$region",
        totalSales: { $sum: "$amount" },
        orderCount: { $sum: 1 }
    });
    // sort & limit
    aggBuilder.sort({ totalSales: -1 });
    aggBuilder.limit(5);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 1 SQL:", sql);
    Logger.info("测试 1 Params:", params);

    // 断言
    assert.ok(sql.includes("GROUP BY"), "SQL 应包含 GROUP BY 子句");
    assert.ok(sql.includes("SUM(amount) AS totalSales"), "SQL 应包含聚合字段 totalSales");
    assert.ok(sql.includes("ORDER BY totalSales DESC"), "SQL 应包含对 totalSales 的降序排序");
    assert.ok(sql.includes("LIMIT 5"), "SQL 应包含 LIMIT 5");
}

/* ============================================================
   测试 2：多次 lookup（多表联结），再聚合
============================================================ */
function testMultipleLookups() {
    Logger.info("测试 2：多次 lookup（多表联结）聚合");

    const aggBuilder = new MongoAggregationBuilder("orders");

    // match：订单日期在 2024 年
    aggBuilder.match({ orderDate: { $gte: "2024-01-01", $lte: "2024-12-31" } });

    // lookup：关联 customer
    aggBuilder.lookup({
        from: "customer",
        localField: "customerId",
        foreignField: "id",
        as: "customer"
    });
    // lookup：关联 products
    aggBuilder.lookup({
        from: "products",
        localField: "productId",
        foreignField: "id",
        as: "product"
    });

    // match：只保留 product.category = "Electronics"
    aggBuilder.match({ "product.category": "Electronics" });

    // group：按 customerId，统计 totalSales, orderCount
    aggBuilder.group({
        _id: "$customerId",
        totalSales: { $sum: "$amount" },
        orderCount: { $sum: 1 }
    });

    aggBuilder.sort({ totalSales: -1 });
    aggBuilder.limit(10);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 2 SQL:", sql);
    Logger.info("测试 2 Params:", params);

    assert.ok(sql.includes("LEFT JOIN customer"), "SQL 应包含 LEFT JOIN customer");
    assert.ok(sql.includes("LEFT JOIN products"), "SQL 应包含 LEFT JOIN products");
    assert.ok(sql.includes("GROUP BY"), "SQL 应包含 GROUP BY 子句");
    assert.ok(sql.includes("SUM(amount) AS totalSales"), "SQL 应包含聚合字段 totalSales");
    assert.ok(sql.includes("ORDER BY totalSales DESC"), "SQL 应包含对 totalSales 的降序排序");
}

/* ============================================================
   测试 3：只使用 $project + $match
============================================================ */
function testProjectAndMatch() {
    Logger.info("测试 3：只使用 $match + $project");

    const aggBuilder = new MongoAggregationBuilder("users");
    aggBuilder.match({
        age: { $gt: 25 },
        location: "NY"
    });
    aggBuilder.project(["id", "name", "email"]);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 3 SQL:", sql);
    Logger.info("测试 3 Params:", params);

    assert.ok(!sql.includes("GROUP BY"), "SQL 不应包含 GROUP BY");
    // 如果你实现了投影转换，可断言具体 SELECT 片段
    assert.ok(true, "仅测试 $project + $match，无分组");
}

/* ============================================================
   测试 4：$unwind 示例
============================================================ */
function testUnwindExample() {
    Logger.info("测试 4：$unwind");
    const aggBuilder = new MongoAggregationBuilder("orders");
    aggBuilder.match({ status: "completed" });
    aggBuilder.unwind("items");
    aggBuilder.group({
        _id: "$items.productId",
        totalQuantity: { $sum: "$items.quantity" }
    });
    aggBuilder.limit(5);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 4 SQL:", sql);
    Logger.info("测试 4 Params:", params);

    assert.ok(sql.includes("UNWIND(items)"), "SQL 中应包含 UNWIND(items) 标记");
    assert.ok(sql.includes("SUM(items.quantity) AS totalQuantity"), "SQL 中应包含 SUM(items.quantity) AS totalQuantity");
}

/* ============================================================
   测试 5：普通连表查询（非聚合）
============================================================ */
function testNormalJoins() {
    Logger.info("测试 5：普通连表查询");

    const builder = new MongoQueryBuilder("orders");
    builder.query({ status: "active" })
        .project(["orders.id", "orders.amount", "customer.name"])
        .join([{
            tableName: "customer",
            joinType: "LEFT JOIN",
            alias: "customer",
            on: "orders.customerId = customer.id"
        }])
        .sort("orders.createdAt DESC")
        .limit(10);

    const { sql, params } = builder.toSQL();
    Logger.info("测试 5 SQL:", sql);
    Logger.info("测试 5 Params:", params);

    assert.ok(sql.includes("LEFT JOIN customer"), "SQL 应包含 LEFT JOIN customer");
    assert.ok(sql.includes("customer.name AS customer_name"), "SQL 应包含 customer.name AS customer_name");
    assert.ok(sql.includes("ORDER BY orders.createdAt DESC"), "SQL 应包含 ORDER BY orders.createdAt DESC");
    assert.ok(sql.includes("LIMIT 10"), "SQL 应包含 LIMIT 10");
}

/* ============================================================
   测试 6：统计某一天开始后面日期的用户登录情况
============================================================ */
function testUserLoginStats() {
    Logger.info("测试 6：统计某一天开始后面日期的用户登录情况");

    const aggBuilder = new MongoAggregationBuilder("user_logins");

    // 1. match：只保留 2025-01-01 及之后的登录记录
    aggBuilder.match({ loginDate: { $gte: "2025-01-01" } });
    // 2. group：按 loginDate 分组，统计当天的登录次数
    aggBuilder.group({
        _id: "$loginDate",
        loginCount: { $sum: 1 }
    });
    // 3. sort：按日期升序
    aggBuilder.sort({ _id: 1 });
    // 4. limit：只显示前 30 天
    aggBuilder.limit(30);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 6 SQL:", sql);
    Logger.info("测试 6 Params:", params);

    assert.ok(sql.includes("GROUP BY"), "SQL 中应包含 GROUP BY 子句");
    assert.ok(sql.includes("SUM(1) AS loginCount"), "SQL 中应包含 SUM(1) AS loginCount");
    assert.ok(sql.includes("LIMIT 30"), "SQL 中应包含 LIMIT 30");
}

/* ============================================================
   测试 7：更加复杂的聚合场景
============================================================ */
function testComplexAggregationScenario() {
    Logger.info("测试 7：更加复杂的聚合场景");

    const aggBuilder = new MongoAggregationBuilder("events");

    // 1. match：type in ["A", "B"] & eventTime 在 2025-01-01 ~ 2025-03-01
    aggBuilder.match({
        type: { $in: ["A", "B"] },
        eventTime: { $gte: "2025-01-01", $lte: "2025-03-01" }
    });

    // 2. unwind: events.tags
    aggBuilder.unwind("tags");

    // 3. match：tags = "important"
    aggBuilder.match({ tags: "important" });

    // 4. lookup：关联 users 表
    aggBuilder.lookup({
        from: "users",
        localField: "userId",
        foreignField: "id",
        as: "userInfo"
    });

    // 5. group：按 userId 分组，计算多个字段
    aggBuilder.group({
        _id: "$userId",
        totalEvents: { $sum: 1 },
        earliestEventTime: { $min: "$eventTime" },
        latestEventTime: { $max: "$eventTime" }
    });

    // 6. sort：按 totalEvents 降序
    aggBuilder.sort({ totalEvents: -1 });
    // 7. skip：10
    aggBuilder.skip(10);
    // 8. limit：5
    aggBuilder.limit(5);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 7 SQL:", sql);
    Logger.info("测试 7 Params:", params);

    assert.ok(sql.includes("UNWIND(tags)"), "SQL 中应包含 UNWIND(tags) 标记");
    assert.ok(sql.includes("LEFT JOIN users"), "SQL 中应包含 LEFT JOIN users");
    assert.ok(sql.includes("GROUP BY"), "SQL 中应包含 GROUP BY");
    assert.ok(sql.includes("SUM(1) AS totalEvents"), "SQL 中应包含 SUM(1) AS totalEvents");
    assert.ok(sql.includes("MIN(eventTime) AS earliestEventTime"), "SQL 中应包含 MIN(eventTime) AS earliestEventTime");
    assert.ok(sql.includes("MAX(eventTime) AS latestEventTime"), "SQL 中应包含 MAX(eventTime) AS latestEventTime");
    assert.ok(sql.includes("ORDER BY totalEvents DESC"), "SQL 中应包含 ORDER BY totalEvents DESC");
    assert.ok(sql.includes("LIMIT 5"), "SQL 中应包含 LIMIT 5");
    assert.ok(sql.includes("OFFSET 10"), "SQL 中应包含 OFFSET 10");
}

/* ============================================================
   测试 8：登录-下单-复购漏斗测试
============================================================ */
/**
 * 统计在 2025-01-01 登录并下单、且在 2025-01-02 或 2025-01-03 再次下单的用户
 */
function testLoginPurchaseFunnel() {
    Logger.info("测试 8：登录-下单-复购漏斗");

    const aggBuilder = new MongoAggregationBuilder("user_logins");

    // 1) 保留 2025-01-01 登录记录
    aggBuilder.match({
        loginDate: "2025-01-01"
    });

    // 2) lookup orders: day1Orders
    aggBuilder.lookup({
        from: "orders",
        localField: "userId",
        foreignField: "userId",
        as: "day1Orders"
    });

    // 3) match: day1Orders 中必须有 orderDate = "2025-01-01"
    aggBuilder.match({
        "day1Orders.orderDate": "2025-01-01"
    });

    // 4) 再次 lookup orders: futureOrders
    aggBuilder.lookup({
        from: "orders",
        localField: "userId",
        foreignField: "userId",
        as: "futureOrders"
    });

    // 5) match: futureOrders.orderDate ∈ ["2025-01-02", "2025-01-03"]
    aggBuilder.match({
        "futureOrders.orderDate": { $in: ["2025-01-02", "2025-01-03"] }
    });

    // 6) 只投影 user_logins.userId
    aggBuilder.project(["user_logins.userId"]);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 8 SQL:", sql);
    Logger.info("测试 8 Params:", params);

    // 简单断言
    assert.ok(sql.includes("LEFT JOIN orders AS day1Orders"), "SQL 中应出现 day1Orders JOIN");
    assert.ok(sql.includes("LEFT JOIN orders AS futureOrders"), "SQL 中应出现 futureOrders JOIN");
    assert.ok(sql.includes("day1Orders.orderDate = ?"), "SQL 中应包含对 2025-01-01 的筛选");
    assert.ok(sql.includes("futureOrders.orderDate IN"), "SQL 中应包含对 2025-01-02/2025-01-03 的筛选");
    Logger.info("测试 8：登录-下单-复购漏斗通过\n");
}

/* ============================================================
   测试 9：更多统计 / 计算类聚合示例
============================================================ */
/**
 * 1) testAveragePurchaseValue
 *    - 需求：计算用户的平均下单金额
 *    - 表结构：orders(userId, amount, ...)
 */
function testAveragePurchaseValue() {
    Logger.info("测试 9.1：计算用户的平均下单金额");

    const aggBuilder = new MongoAggregationBuilder("orders");

    // match：假设只统计 2025 年的数据
    aggBuilder.match({
        orderDate: { $gte: "2025-01-01", $lte: "2025-12-31" }
    });

    // group：按 userId 分组，计算平均金额 avgAmount
    aggBuilder.group({
        _id: "$userId",
        avgAmount: { $avg: "$amount" }
    });

    // sort：按平均金额降序
    aggBuilder.sort({ avgAmount: -1 });
    // limit：只显示前 10
    aggBuilder.limit(10);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 9.1 SQL:", sql);
    Logger.info("测试 9.1 Params:", params);

    // 断言
    assert.ok(sql.includes("AVG(amount) AS avgAmount"), "SQL 中应包含 AVG(amount) AS avgAmount");
    assert.ok(sql.includes("GROUP BY"), "SQL 中应包含 GROUP BY");
    assert.ok(sql.includes("ORDER BY avgAmount DESC"), "SQL 应包含对 avgAmount 的降序排序");
}

/**
 * 2) testMinMaxUserActivity
 *    - 需求：统计各用户最早 & 最晚活跃时间
 *    - 表结构：activity(userId, activityTime, ...)
 */
function testMinMaxUserActivity() {
    Logger.info("测试 9.2：统计各用户最早 & 最晚活跃时间");

    const aggBuilder = new MongoAggregationBuilder("activity");

    // match：仅查看 2025 年之后的活动
    aggBuilder.match({
        activityTime: { $gte: "2025-01-01" }
    });

    // group：按 userId 分组，计算 earliestActivity, latestActivity
    aggBuilder.group({
        _id: "$userId",
        earliestActivity: { $min: "$activityTime" },
        latestActivity: { $max: "$activityTime" }
    });

    // sort：按 userId 升序
    aggBuilder.sort({ _id: 1 });

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("测试 9.2 SQL:", sql);
    Logger.info("测试 9.2 Params:", params);

    assert.ok(sql.includes("MIN(activityTime) AS earliestActivity"), "SQL 中应包含 MIN(activityTime) AS earliestActivity");
    assert.ok(sql.includes("MAX(activityTime) AS latestActivity"), "SQL 中应包含 MAX(activityTime) AS latestActivity");
    assert.ok(sql.includes("GROUP BY"), "SQL 中应包含 GROUP BY");
}

/* ============================================================
   统一运行函数
============================================================ */
function runAllTests() {
    try {
        testBasicAggregation();            // 测试 1
        testMultipleLookups();             // 测试 2
        testProjectAndMatch();             // 测试 3
        testUnwindExample();               // 测试 4
        testNormalJoins();                 // 测试 5
        testUserLoginStats();              // 测试 6
        testComplexAggregationScenario();  // 测试 7
        testLoginPurchaseFunnel();         // 测试 8
        testAveragePurchaseValue();        // 测试 9.1
        testMinMaxUserActivity();          // 测试 9.2
        console.log("聚合和连表测试全部通过！");
    } catch (err) {
        console.error("测试失败：", err);
        process.exit(1);
    }
}

// 如果直接执行本文件，则运行所有测试
if (require.main === module) {
    runAllTests();
}

module.exports = {
    runAllTests
};
