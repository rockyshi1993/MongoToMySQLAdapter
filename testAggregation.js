"use strict";

/**
 * 本示例展示了使用 MongoAggregationBuilder 构造聚合查询，并转换为对应的 MySQL SQL 语句。
 * 涉及的场景包括：
 * 1. 简单聚合：$match + $group + $project + $sort + $limit + $skip
 * 2. 使用 $lookup 进行跨表关联
 * 3. 复杂聚合：多个 $lookup、附加 $match、分组统计、分页等操作
 * 4. 只使用 $match 和 $project 的聚合操作
 * 5. 错误处理示例：使用不支持的操作符（如 $median）以触发错误
 * 6. 使用 $unwind 展开数组字段的聚合操作
 */

const {
    MongoAggregationBuilder,
    SQLGenerationError,
    Logger
} = require("./index");
const assert = require("assert");

/**
 * 示例 1：简单聚合操作
 *
 * 统计 "sales" 表中，地区为 "North" 或 "South" 且销售额大于 0 的记录，
 * 按地区分组，计算每个地区的总销售额和订单数量，
 * 最后按总销售额降序排序，并返回前 5 个结果（分页：跳过 0 条记录）。
 */
function exampleSimpleAggregation() {
    Logger.info("示例 1：简单聚合操作");

    const aggBuilder = new MongoAggregationBuilder("sales");

    aggBuilder.match({
        amount: { $gt: 0 },
        region: { $in: ["North", "South"] }
    });

    aggBuilder.group({
        _id: "$region",
        totalSales: { $sum: "$amount" },
        orderCount: { $sum: 1 }
    });

    aggBuilder.project(["_id", "totalSales", "orderCount"]);
    aggBuilder.sort({ totalSales: -1 });
    aggBuilder.limit(5);
    aggBuilder.skip(0);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("示例 1 SQL:", sql);
    Logger.info("示例 1 参数:", params);
}

/**
 * 示例 2：使用 $lookup 的聚合操作
 *
 * 从 "orders" 表中统计订单数据，
 * 筛选订单状态为 "completed"，然后通过 $lookup 关联 "customer" 表，
 * 按 customerId 分组统计每个客户的订单总数和订单总额，
 * 最后按订单总额降序排序，返回前 10 个结果。
 */
function exampleLookupAggregation() {
    Logger.info("示例 2：使用 $lookup 的聚合操作");

    const aggBuilder = new MongoAggregationBuilder("orders");

    aggBuilder.match({ status: "completed" });
    aggBuilder.lookup({
        from: "customer",
        localField: "customerId",
        foreignField: "id",
        as: "customer"
    });

    aggBuilder.group({
        _id: "$customerId",
        totalAmount: { $sum: "$amount" },
        orderCount: { $sum: 1 }
    });

    aggBuilder.project(["_id", "totalAmount", "orderCount"]);
    aggBuilder.sort({ totalAmount: -1 });
    aggBuilder.limit(10);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("示例 2 SQL:", sql);
    Logger.info("示例 2 参数:", params);
}

/**
 * 示例 3：复杂聚合操作
 *
 * 假设有三个表：订单表 "orders"、客户表 "customer"、产品表 "products"。
 * 统计每个客户在 2024 年内购买 "Electronics" 类别产品的订单情况，
 * 处理流程：
 * 1. $match：筛选订单状态为 "completed" 且订单日期在 2024 年内。
 * 2. $lookup：关联 "customer" 表，获取客户信息。
 * 3. $lookup：关联 "products" 表，获取产品信息。
 * 4. 附加 $match：过滤出产品类别为 "Electronics" 的记录。
 * 5. $group：按 customerId 分组，统计总销售额和订单数量。
 * 6. $project：输出分组统计结果。
 * 7. $sort：按订单数量降序排序。
 * 8. $limit 与 $skip：分页处理（例如返回第 2 页，每页 10 条记录）。
 */
function exampleComplexAggregation() {
    Logger.info("示例 3：复杂聚合操作");

    const aggBuilder = new MongoAggregationBuilder("orders");

    aggBuilder.match({
        status: "completed",
        orderDate: { $gte: "2024-01-01", $lte: "2024-12-31" }
    });

    aggBuilder.lookup({
        from: "customer",
        localField: "customerId",
        foreignField: "id",
        as: "customer"
    });

    aggBuilder.lookup({
        from: "products",
        localField: "productId",
        foreignField: "id",
        as: "product"
    });

    // 过滤产品类别为 "Electronics"
    aggBuilder.match({ "product.category": "Electronics" });

    aggBuilder.group({
        _id: "$customerId",
        totalSales: { $sum: "$amount" },
        orderCount: { $sum: 1 }
    });

    aggBuilder.project(["_id", "totalSales", "orderCount"]);
    aggBuilder.sort({ orderCount: -1 });
    aggBuilder.limit(10);
    aggBuilder.skip(10);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("示例 3 SQL:", sql);
    Logger.info("示例 3 参数:", params);
}

/**
 * 示例 4：只使用 $match 和 $project 的聚合操作
 *
 * 针对 "users" 表，筛选年龄大于 20 且性别为 "female" 的用户，
 * 仅返回 id、name 和 email 字段。
 */
function exampleMatchProjectAggregation() {
    Logger.info("示例 4：只使用 $match 和 $project 的聚合操作");

    const aggBuilder = new MongoAggregationBuilder("users");

    aggBuilder.match({
        age: { $gt: 20 },
        gender: "female"
    });

    aggBuilder.project(["id", "name", "email"]);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("示例 4 SQL:", sql);
    Logger.info("示例 4 参数:", params);
}

/**
 * 示例 5：聚合操作错误处理示例
 *
 * 尝试使用不支持的聚合操作符（例如 $median），期望抛出 SQLGenerationError。
 */
function exampleAggregationError() {
    Logger.info("示例 5：聚合操作错误处理测试");

    try {
        const aggBuilder = new MongoAggregationBuilder("sales");
        aggBuilder.group({
            _id: "$region",
            medianSales: { $median: "$amount" }
        });
        aggBuilder.toSQL();
        throw new Error("预期错误未被抛出");
    } catch (err) {
        Logger.info("示例 5 捕获预期错误:", err.message);
        if (!(err instanceof SQLGenerationError)) {
            throw err;
        }
    }
}

/**
 * 示例 6：$unwind 聚合操作
 *
 * 从 "orders" 表中筛选状态为 "completed" 的订单，
 * 使用 $unwind 展开订单中的 items 数组字段，
 * 然后按每个商品 ID 分组，累计 items.quantity，
 * 最后按累计数量降序排序，返回前 5 个结果。
 */
function exampleUnwindAggregation() {
    Logger.info("示例 6：$unwind 聚合操作测试");

    const aggBuilder = new MongoAggregationBuilder("orders");
    // $match：筛选订单状态为 "completed"
    aggBuilder.match({ status: "completed" });
    // $unwind：展开数组字段 "items"
    aggBuilder.unwind("items");

    // $group：按 items.productId 分组，累计 items.quantity
    aggBuilder.group({
        _id: "$items.productId",
        totalQuantity: { $sum: "$items.quantity" }
    });
    aggBuilder.project(["_id", "totalQuantity"]);
    aggBuilder.sort({ totalQuantity: -1 });
    aggBuilder.limit(5);

    const { sql, params } = aggBuilder.toSQL();
    Logger.info("示例 6 SQL:", sql);
    Logger.info("示例 6 参数:", params);

    // 检查生成的 SQL 中是否包含 UNWIND 的标记（此处以注释方式存在）
    assert.ok(sql.includes("UNWIND"), "SQL 中必须包含 UNWIND 标记");
}

/**
 * 主函数：依次运行所有聚合示例
 */
function runAggregationExamples() {
    try {
        exampleSimpleAggregation();
        exampleLookupAggregation();
        exampleComplexAggregation();
        exampleMatchProjectAggregation();
        exampleAggregationError();
        exampleUnwindAggregation();
        console.log("所有聚合操作示例均已通过！");
    } catch (err) {
        console.error("聚合操作示例测试失败：", err);
        process.exit(1);
    }
}

// 当直接执行该文件时运行所有示例
if (require.main === module) {
    runAggregationExamples();
}

module.exports = {
    runAggregationExamples
};
