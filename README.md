# MongoSQL Builder 使用说明

MongoSQL Builder 是一个用于将 MongoDB 风格的查询、聚合、更新、删除、插入操作转换为对应 MySQL 查询语句的 npm 包。

> **注意：**
> - 内部存储查询条件的属性已改为 `this.filter`，避免与 `query()` 方法冲突。
> - 更新操作使用 `.update()` 方法，支持传入对象（固定更新，默认视为 `$set`）或数组（批量更新）。可通过 `.single()` 限制仅更新第一条记录。
> - 插入操作中，使用 `.insertOne()` 进行单条插入，使用 `.insertMany()` 进行批量插入，并支持 upsert 操作。
> - 聚合查询目前支持 `$sum`、`$avg`、`$min`、`$max` 等基本聚合函数。扩展支持字段加、减、乘、除等运算可通过扩展辅助解析函数实现。

---

## 目录

1. [MongoQueryBuilder (SELECT 查询)](#mongoquerybuilder-select-查询)
2. [MongoAggregationBuilder (聚合查询)](#mongoaggregationbuilder-聚合查询)
3. [MongoUpdateBuilder (更新操作)](#mongoupdatebuilder-更新操作)
4. [MongoDeleteBuilder (删除操作)](#mongodeletebuilder-删除操作)
5. [MongoInsertBuilder (插入操作)](#mongoinsertbuilder-插入操作)
6. [SubQuery (子查询)](#subquery-子查询)
7. [错误情况与 Offset 示例](#错误情况與-offset-示例)
8. [聚合查询扩展说明](#聚合查询扩展说明)

---

## MongoQueryBuilder (SELECT 查询)

**功能说明：**  

### 示例

```javascript
// 查询 users 表中 age > 18 且 name LIKE '%John%' 的记录，
// 按 age 升序排序，返回 10 条记录，从第 0 条开始。
const queryExample = new MongoQueryBuilder("users")
  .query({ age: { $gt: 18 } })
  .query({ name: { $like: "%John%" } })
  .sort("age ASC")
  .limit(10)
  .offset(0);
const querySQL = queryExample.toSQL();
console.log("Query SQL:", querySQL);
