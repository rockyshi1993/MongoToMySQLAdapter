"use strict";

// DEBUG 开关，调试时置为 true（生产环境可置为 false）
const DEBUG = true;
function logDebug(...args) {
    if (DEBUG) console.debug(...args);
}

// 定义 MongoDB 操作符与 MySQL 操作符的映射
const OPERATORS_MYSQL = {
    $eq: '=',
    $ne: '<>',
    $gt: '>',
    $gte: '>=',
    $lt: '<',
    $lte: '<=',
    $in: 'IN',
    $nin: 'NOT IN',
    $and: 'AND',
    $or: 'OR',
    $not: 'NOT',
    $regex: 'REGEXP',
    $like: 'LIKE'
};

/* ============================================================
   SELECT 查询转换及辅助函数
============================================================*/

/**
 * parseMongoQuery(query, params)
 * 将 MongoDB 查询对象转换为 SQL WHERE 子句字符串，并收集参数。
 *
 * @param {Object} query - MongoDB 查询对象，例如 { age: { $gt: 18 } }
 * @param {Array} params - 用于收集参数的数组
 * @returns {String} SQL 条件字符串（不含 "WHERE"）
 */
function parseMongoQuery(query, params) {
    let conditions = [];
    for (let key in query) {
        const value = query[key];
        try {
            if (key.startsWith('$')) {
                logDebug("处理逻辑操作符:", key, value);
                handleLogicalOperators(key, value, conditions, params);
            } else {
                if (value instanceof SubQuery) {
                    // 例如：{ id: { $in: subQuery } }
                    logDebug("检测到子查询在字段:", key);
                    const subResult = value.toSQL();
                    conditions.push(`${key} IN ${subResult}`);
                    params.push(...value.getParams());
                } else if (typeof value === 'object' && !Array.isArray(value)) {
                    let fieldConds = [];
                    for (let op in value) {
                        if (op.startsWith('$')) {
                            // 如果操作符是 $in 且值为 SubQuery，特殊处理
                            if (op === "$in" && value[op] instanceof SubQuery) {
                                logDebug("检测到子查询 in $in operator:", key);
                                const subResult = value[op].toSQL();
                                fieldConds.push(`${key} IN ${subResult}`);
                                params.push(...value[op].getParams());
                            } else if (['$in', '$nin', '$all'].includes(op)) {
                                handleArrayOperators(key, value[op], op, fieldConds, params);
                            } else {
                                handleOperator(key, value[op], op, fieldConds, params);
                            }
                        } else {
                            fieldConds.push(`${key} = ?`);
                            params.push(value[op]);
                        }
                    }
                    if (fieldConds.length > 1) {
                        conditions.push(`(${fieldConds.join(" AND ")})`);
                    } else if (fieldConds.length === 1) {
                        conditions.push(fieldConds[0]);
                    }
                } else {
                    conditions.push(`${key} = ?`);
                    params.push(value);
                }
            }
        } catch (e) {
            console.error(`Error processing key "${key}": ${e.message}`);
            throw e;
        }
    }
    const result = conditions.length ? conditions.join(" AND ") : "1=1";
    logDebug("生成的 WHERE 子句:", result, "参数:", params);
    return result;
}

function handleLogicalOperators(operator, value, conditions, params) {
    if (!Array.isArray(value) || value.length === 0) {
        conditions.push(operator === '$and' ? "1=1" : "1=0");
        return;
    }
    let subConds = value.map(subQuery => "(" + parseMongoQuery(subQuery, params) + ")");
    if (operator === '$nor') {
        conditions.push("NOT (" + subConds.join(" OR ") + ")");
    } else {
        conditions.push("(" + subConds.join(" " + (OPERATORS_MYSQL[operator] || operator) + " ") + ")");
    }
    logDebug(`逻辑操作符 ${operator} 处理后:`, conditions[conditions.length - 1]);
}

function handleArrayOperators(field, values, operator, conditions, params) {
    if (!Array.isArray(values) || values.length === 0) return;
    if (values.some(v => v instanceof SubQuery)) {
        let subQueries = values.map(v => {
            if (v instanceof SubQuery) {
                params.push(...v.getParams());
                return v.toSQL();
            } else {
                params.push(v);
                return "?";
            }
        });
        conditions.push(`${field} IN (${subQueries.join(", ")})`);
    } else {
        if (operator === '$in') {
            let placeholders = values.map(val => { params.push(val); return "?"; }).join(", ");
            conditions.push(`${field} IN (${placeholders})`);
        } else if (operator === '$nin') {
            let placeholders = values.map(val => { params.push(val); return "?"; }).join(", ");
            conditions.push(`${field} NOT IN (${placeholders})`);
        } else if (operator === '$all') {
            let subConds = values.map(val => {
                params.push(JSON.stringify(val));
                return `JSON_CONTAINS(${field}, ?)`;
            });
            conditions.push("(" + subConds.join(" AND ") + ")");
        }
    }
    logDebug(`数组操作符 ${operator} 处理后 for field ${field}:`, conditions[conditions.length - 1]);
}

function handleOperator(field, opValue, operator, conditions, params) {
    if (opValue instanceof SubQuery) {
        const subSql = opValue.toSQL();
        conditions.push(`${field} IN ${subSql}`);
        params.push(...opValue.getParams());
        logDebug(`子查询处理 for field ${field}:`, subSql);
        return;
    }
    switch (operator) {
        case '$exists':
            conditions.push(`${field} ${opValue ? "IS NOT NULL" : "IS NULL"}`);
            break;
        case '$size':
            conditions.push(`JSON_LENGTH(${field}) = ?`);
            params.push(opValue);
            break;
        case '$elemMatch':
            conditions.push(`JSON_CONTAINS(${field}, ?)`);
            params.push(JSON.stringify(opValue));
            break;
        case '$regex':
            conditions.push(`${field} REGEXP ?`);
            params.push(opValue);
            break;
        case '$like':
            conditions.push(`${field} LIKE ?`);
            params.push(opValue);
            break;
        default:
            let sqlOperator = OPERATORS_MYSQL[operator];
            if (!sqlOperator) {
                throw new Error(`Unsupported operator: ${operator} for field ${field}`);
            }
            conditions.push(`${field} ${sqlOperator} ?`);
            params.push(opValue);
    }
    logDebug(`操作符 ${operator} 处理后 for field ${field}:`, conditions[conditions.length - 1]);
}

/**
 * mongoToMySQL(query, tableName, limit, orderBy)
 * 将 MongoDB 查询对象转换为 SQL SELECT 语句及参数数组。
 *
 * @param {Object} query - 查询条件
 * @param {String} tableName - 表名
 * @param {Number} limit - 返回记录数限制（默认为 10）
 * @param {String} orderBy - 排序条件（默认为 'id DESC'）
 * @returns {Object} { sql, params }
 */
function mongoToMySQL(query, tableName, limit = 10, orderBy = 'id DESC') {
    let params = [];
    const whereClause = parseMongoQuery(query, params);
    const sql = `SELECT * FROM ${tableName} WHERE ${whereClause} ORDER BY ${orderBy} LIMIT ${limit}`;
    logDebug("基本查询 SQL:", sql, "参数:", params);
    return { sql, params };
}

/* ============================================================
   连表查询与 Projection 解析部分
============================================================*/
function prepareJoinMappings(joinConfigs) {
    const mappings = {};
    joinConfigs.forEach(join => {
        let alias = join.alias || join.tableName;
        mappings[alias] = {
            tableName: join.tableName,
            joinType: join.joinType,
            on: join.on,
            alias: alias
        };
    });
    logDebug("生成的连表映射:", mappings);
    return mappings;
}

function parseProjectStageWithJoinsOptimized(projectFields, joinMappings, mainTableName) {
    let fields = [];
    projectFields.forEach(key => {
        if (key.indexOf('.') !== -1) {
            let parts = key.split(".");
            let alias = parts[0];
            let col = parts[1];
            if (joinMappings[alias]) {
                fields.push(`${alias}.${col} AS ${alias}_${col}`);
            } else {
                fields.push(`${mainTableName}.${key} AS ${mainTableName}_${key}`);
            }
        } else {
            fields.push(`${mainTableName}.${key} AS ${mainTableName}_${key}`);
        }
    });
    const result = fields.join(", ");
    logDebug("生成的投影 SELECT 片段:", result);
    return result;
}

function generateJoinClause(joinConfigs) {
    const joinClause = joinConfigs.map(join => {
        const aliasPart = join.alias ? ('AS ' + join.alias) : '';
        return ` ${join.joinType} ${join.tableName} ${aliasPart} ON ${join.on}`;
    }).join(" ");
    logDebug("生成的 JOIN 子句:", joinClause);
    return joinClause;
}

/**
 * mongoToMySQLWithJoinsOptimized(query, tableName, joinConfigs, limit, orderBy)
 * 生成包含连表查询的 SQL 语句。
 *
 * @param {Object} query - 包含查询条件的 MongoDB 查询对象（可包含 $project 阶段）
 * @param {String} tableName - 主表名称
 * @param {Array} joinConfigs - 连表配置数组
 * @param {Number} limit - 记录数限制
 * @param {String} orderBy - 排序条件
 * @returns {Object} { sql, params }
 */
function mongoToMySQLWithJoinsOptimized(query, tableName, joinConfigs = [], limit = 10, orderBy = 'id DESC') {
    let params = [];
    const joinMappings = prepareJoinMappings(joinConfigs);
    let selectClause = "*";
    if (query.$project) {
        selectClause = parseProjectStageWithJoinsOptimized(query.$project, joinMappings, tableName);
        delete query.$project;
    }
    const whereClause = parseMongoQuery(query, params);
    const joinClause = generateJoinClause(joinConfigs);
    const sql = `SELECT ${selectClause} FROM ${tableName}${joinClause} WHERE ${whereClause} ORDER BY ${orderBy} LIMIT ${limit}`;
    logDebug("生成的连表查询 SQL:", sql, "参数:", params);
    return { sql, params };
}

/* ============================================================
   子查询支持
============================================================*/
/**
 * SubQuery 类
 * 用于构造子查询，将 MongoQueryBuilder 生成的 SQL 嵌入到主查询中。
 */
class SubQuery {
    constructor(queryBuilder) {
        this.queryBuilder = queryBuilder;
    }
    toSQL() {
        let result = this.queryBuilder.toSQL();
        return "(" + result.sql + ")";
    }
    getParams() {
        return this.queryBuilder.toSQL().params;
    }
}

/* ============================================================
   聚合查询支持
============================================================*/
/**
 * parseGroupStage(groupObj)
 * 解析 $group 阶段对象，生成 SELECT 字段部分和 GROUP BY 子句。
 *
 * @param {Object} groupObj - 例如 { _id: "$department", total: { $sum: "$salary" } }
 * @returns {Object} { selectClause, groupByClause, havingClause }
 */
function parseGroupStage(groupObj) {
    let selectParts = [];
    let groupByParts = [];
    let havingParts = [];
    if (groupObj._id) {
        if (typeof groupObj._id === "string" && groupObj._id.startsWith("$")) {
            const field = groupObj._id.substring(1);
            selectParts.push(`${field} AS _id`);
            groupByParts.push(field);
        } else {
            selectParts.push(`'${groupObj._id}' AS _id`);
        }
    }
    for (let key in groupObj) {
        if (key === "_id") continue;
        const operatorObj = groupObj[key];
        const operator = Object.keys(operatorObj)[0];
        const operand = operatorObj[operator];
        let sqlFunc = "";
        switch (operator) {
            case "$sum":
                sqlFunc = "SUM";
                break;
            case "$avg":
                sqlFunc = "AVG";
                break;
            case "$min":
                sqlFunc = "MIN";
                break;
            case "$max":
                sqlFunc = "MAX";
                break;
            default:
                throw new Error("不支持的聚合操作符: " + operator);
        }
        let operandStr = "";
        if (typeof operand === "string" && operand.startsWith("$")) {
            operandStr = operand.substring(1);
        } else {
            operandStr = operand;
        }
        selectParts.push(`${sqlFunc}(${operandStr}) AS ${key}`);
    }
    return {
        selectClause: selectParts.join(", "),
        groupByClause: groupByParts.join(", "),
        havingClause: havingParts.join(" AND ")
    };
}

/**
 * MongoAggregationBuilder 类
 * 构造 MongoDB 风格的聚合查询管道，生成对应的 SQL 语句。
 */
class MongoAggregationBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.pipeline = [];
    }
    match(query) {
        this.pipeline.push({ $match: query });
        return this;
    }
    group(groupObj) {
        this.pipeline.push({ $group: groupObj });
        return this;
    }
    project(projection) {
        this.pipeline.push({ $project: projection });
        return this;
    }
    sort(sortObj) {
        this.pipeline.push({ $sort: sortObj });
        return this;
    }
    limit(n) {
        this.pipeline.push({ $limit: n });
        return this;
    }
    skip(n) {
        this.pipeline.push({ $skip: n });
        return this;
    }
    toSQL() {
        let params = [];
        let whereClause = "";
        let groupClause = "";
        let havingClause = "";
        let selectClause = "*";
        let orderClause = "";
        let limitClause = "";
        let offsetClause = "";

        for (let stage of this.pipeline) {
            if (stage.$match) {
                whereClause = parseMongoQuery(stage.$match, params);
            } else if (stage.$group) {
                const groupResult = parseGroupStage(stage.$group);
                selectClause = groupResult.selectClause;
                groupClause = groupResult.groupByClause;
                havingClause = groupResult.havingClause;
            } else if (stage.$sort) {
                if (typeof stage.$sort === "object") {
                    let sortArr = [];
                    for (let key in stage.$sort) {
                        let direction = stage.$sort[key] === -1 ? "DESC" : "ASC";
                        sortArr.push(`${key} ${direction}`);
                    }
                    orderClause = sortArr.join(", ");
                } else {
                    orderClause = stage.$sort;
                }
            } else if (stage.$limit) {
                limitClause = stage.$limit;
            } else if (stage.$skip) {
                offsetClause = stage.$skip;
            } else if (stage.$project) {
                if (!groupClause && Array.isArray(stage.$project)) {
                    selectClause = parseProjectStageWithJoinsOptimized(stage.$project, {}, this.tableName);
                }
            }
        }

        let sql = "SELECT " + selectClause + " FROM " + this.tableName;
        if (whereClause) {
            sql += " WHERE " + whereClause;
        }
        if (groupClause) {
            sql += " GROUP BY " + groupClause;
        }
        if (havingClause) {
            sql += " HAVING " + havingClause;
        }
        if (orderClause) {
            sql += " ORDER BY " + orderClause;
        }
        if (limitClause) {
            sql += " LIMIT " + limitClause;
            if (offsetClause) {
                sql += " OFFSET " + offsetClause;
            }
        } else if (offsetClause) {
            sql += " LIMIT 18446744073709551615 OFFSET " + offsetClause;
        }
        logDebug("聚合查询生成的 SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

/* ============================================================
   更新、删除与新增支持
============================================================*/
/**
 * MongoUpdateBuilder 类
 * 支持固定更新（传入对象，默认视为 $set，可明确使用更新操作符）和批量更新（传入数组，每个对象各自更新不同数据，必须包含唯一索引字段）。
 * 可调用 .single() 限制只更新第一条匹配记录。
 */
class MongoUpdateBuilder {
    constructor(tableName, idField = "id") {
        this.tableName = tableName;
        this.idField = idField;
        this.dataObj = null;
        this.dataArray = null;
        this.filter = {}; // 用 filter 存储查询条件
        this.singleUpdate = false;
    }
    update(updateData) {
        if (Array.isArray(updateData)) {
            updateData.forEach(obj => {
                if (!obj.hasOwnProperty(this.idField)) {
                    throw new Error(`每个更新对象必须包含 '${this.idField}' 字段`);
                }
            });
            this.dataArray = updateData;
            this.dataObj = null;
        } else if (typeof updateData === 'object' && updateData !== null) {
            if (Object.keys(updateData).some(key => key.startsWith('$'))) {
                this.dataObj = updateData;
            } else {
                this.dataObj = { $set: updateData };
            }
            this.dataArray = null;
        } else {
            throw new Error("更新数据必须为对象或数组。");
        }
        return this;
    }
    query(queryObj) {
        if (Object.keys(this.filter).length === 0) {
            this.filter = queryObj;
        } else {
            if (!this.filter.$and) {
                this.filter = { $and: [this.filter] };
            }
            this.filter.$and.push(queryObj);
        }
        return this;
    }
    single() {
        this.singleUpdate = true;
        return this;
    }
    toSQL() {
        if (Object.keys(this.filter).length === 0) {
            throw new Error("更新操作必须指定查询条件，防止误更新所有记录。");
        }
        let params = [];
        let sql = "";
        if (this.dataObj) {
            let updateOps = this.dataObj;
            let setClauses = [];
            for (let op in updateOps) {
                switch (op) {
                    case "$set":
                        for (let field in updateOps.$set) {
                            setClauses.push(`${field} = ?`);
                            params.push(updateOps.$set[field]);
                        }
                        break;
                    case "$inc":
                        for (let field in updateOps.$inc) {
                            setClauses.push(`${field} = ${field} + ?`);
                            params.push(updateOps.$inc[field]);
                        }
                        break;
                    case "$unset":
                        for (let field in updateOps.$unset) {
                            setClauses.push(`${field} = NULL`);
                        }
                        break;
                    case "$mul":
                        for (let field in updateOps.$mul) {
                            setClauses.push(`${field} = ${field} * ?`);
                            params.push(updateOps.$mul[field]);
                        }
                        break;
                    default:
                        throw new Error("不支持的更新操作符: " + op);
                }
            }
            if (setClauses.length === 0) {
                throw new Error("未指定更新字段。");
            }
            sql = `UPDATE ${this.tableName} SET ${setClauses.join(", ")}`;
            let whereClause = parseMongoQuery(this.filter, params);
            sql += ` WHERE ${whereClause}`;
        } else if (this.dataArray) {
            if (this.dataArray.length === 0) {
                throw new Error("未指定更新对象。");
            }
            let updateColumns = new Set();
            this.dataArray.forEach(obj => {
                for (let key in obj) {
                    if (key !== this.idField) {
                        updateColumns.add(key);
                    }
                }
            });
            updateColumns = Array.from(updateColumns);
            let setClauses = updateColumns.map(col => {
                let cases = this.dataArray.map(obj => {
                    if (obj.hasOwnProperty(col)) {
                        return `WHEN ? THEN ?`;
                    }
                    return "";
                }).filter(x => x !== "").join(" ");
                return `${col} = CASE ${this.idField} ${cases} ELSE ${col} END`;
            });
            let caseParams = [];
            updateColumns.forEach(col => {
                this.dataArray.forEach(obj => {
                    if (obj.hasOwnProperty(col)) {
                        caseParams.push(obj[this.idField]);
                        caseParams.push(obj[col]);
                    }
                });
            });
            const ids = this.dataArray.map(obj => obj[this.idField]);
            const placeholders = ids.map(() => "?").join(", ");
            let bulkWhereClause = `${this.idField} IN (${placeholders})`;
            let extraParams = [];
            let extraCondition = parseMongoQuery(this.filter, extraParams);
            if (extraCondition !== "1=1") {
                bulkWhereClause += ` AND (${extraCondition})`;
            }
            sql = `UPDATE ${this.tableName} SET ${setClauses.join(", ")}` +
                ` WHERE ${bulkWhereClause}`;
            params = caseParams.concat(ids, extraParams);
        } else {
            throw new Error("未指定更新数据。");
        }
        if (this.singleUpdate) {
            sql += " LIMIT 1";
        }
        logDebug("生成的 UPDATE SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

/**
 * MongoDeleteBuilder 类
 * 构造 DELETE 语句，默认删除所有匹配记录；调用 .single() 后仅删除第一条记录。
 */
class MongoDeleteBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.filter = {}; // 用 filter 存储删除条件
        this.singleDelete = false;
    }
    query(queryObj) {
        if (Object.keys(this.filter).length === 0) {
            this.filter = queryObj;
        } else {
            if (!this.filter.$and) {
                this.filter = { $and: [this.filter] };
            }
            this.filter.$and.push(queryObj);
        }
        return this;
    }
    single() {
        this.singleDelete = true;
        return this;
    }
    toSQL() {
        if (Object.keys(this.filter).length === 0) {
            throw new Error("删除操作必须指定查询条件，防止误删除所有记录。");
        }
        let params = [];
        let whereClause = parseMongoQuery(this.filter, params);
        let sql = `DELETE FROM ${this.tableName} WHERE ${whereClause}`;
        if (this.singleDelete) {
            sql += " LIMIT 1";
        }
        logDebug("生成的 DELETE SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

/**
 * MongoInsertBuilder 类
 * 支持单条插入和批量插入，并支持 upsert 操作。
 * 单条插入使用 .insertOne()，批量插入使用 .insertMany()。
 */
class MongoInsertBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.doc = null;
        this.docs = [];
        this.upsertEnabled = false;
        this.upsertFields = null;
    }
    insertOne(doc) {
        this.doc = doc;
        return this;
    }
    insertMany(docs) {
        this.docs = docs;
        return this;
    }
    upsert(enable = true, fields = null) {
        this.upsertEnabled = enable;
        this.upsertFields = fields;
        return this;
    }
    toSQL() {
        if (this.doc) {
            const keys = Object.keys(this.doc);
            const columns = keys.join(", ");
            const placeholders = keys.map(() => "?").join(", ");
            let sql = `INSERT INTO ${this.tableName} (${columns}) VALUES (${placeholders})`;
            let params = keys.map(key => this.doc[key]);
            if (this.upsertEnabled) {
                const updateFields = this.upsertFields || keys;
                const updateClause = updateFields.map(col => `${col} = VALUES(${col})`).join(", ");
                sql += ` ON DUPLICATE KEY UPDATE ${updateClause}`;
            }
            logDebug("生成的单条 INSERT SQL:", sql, "参数:", params);
            return { sql, params };
        } else if (this.docs && this.docs.length > 0) {
            const keys = Object.keys(this.docs[0]);
            const columns = keys.join(", ");
            const rowPlaceholders = "(" + keys.map(() => "?").join(", ") + ")";
            let sql = `INSERT INTO ${this.tableName} (${columns}) VALUES ${this.docs.map(() => rowPlaceholders).join(", ")}`;
            let params = [];
            this.docs.forEach(doc => {
                keys.forEach(key => {
                    params.push(doc[key]);
                });
            });
            if (this.upsertEnabled) {
                const updateFields = this.upsertFields || keys;
                const updateClause = updateFields.map(col => `${col} = VALUES(${col})`).join(", ");
                sql += ` ON DUPLICATE KEY UPDATE ${updateClause}`;
            }
            logDebug("生成的批量 INSERT SQL:", sql, "参数:", params);
            return { sql, params };
        } else {
            throw new Error("未指定插入的文档。");
        }
    }
}

/* ============================================================
   MongoQueryBuilder 类
   构造 SELECT 查询语句，支持多条件、排序、limit 和 offset。
============================================================*/
class MongoQueryBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.filter = {}; // 用 filter 存储查询条件
        this.projection = null;
        this.joinConfigs = [];
        this.sortClause = "";
        this.limitValue = null;
        this.offsetValue = null;
    }
    query(queryObj) {
        if (Object.keys(this.filter).length === 0) {
            this.filter = queryObj;
        } else {
            if (!this.filter.$and) {
                this.filter = { $and: [this.filter] };
            }
            this.filter.$and.push(queryObj);
        }
        return this;
    }
    project(fields) {
        this.projection = fields;
        return this;
    }
    join(joinConfigs) {
        this.joinConfigs = joinConfigs;
        return this;
    }
    sort(sortBy) {
        this.sortClause = sortBy;
        return this;
    }
    limit(limit) {
        this.limitValue = limit;
        return this;
    }
    offset(offset) {
        this.offsetValue = offset;
        return this;
    }
    toSQL() {
        let params = [];
        let selectClause = "*";
        if (this.projection) {
            if (this.joinConfigs && this.joinConfigs.length > 0) {
                selectClause = parseProjectStageWithJoinsOptimized(this.projection, prepareJoinMappings(this.joinConfigs), this.tableName);
            } else {
                selectClause = parseProjectStageWithJoinsOptimized(this.projection, {}, this.tableName);
            }
        }
        let sql = `SELECT ${selectClause} FROM ${this.tableName}`;
        if (this.joinConfigs && this.joinConfigs.length > 0) {
            sql += generateJoinClause(this.joinConfigs);
        }
        const whereClause = parseMongoQuery(this.filter, params);
        if (whereClause) {
            sql += ` WHERE ${whereClause}`;
        }
        if (this.sortClause) {
            sql += ` ORDER BY ${this.sortClause}`;
        }
        if (this.limitValue !== null) {
            sql += ` LIMIT ${this.limitValue}`;
            if (this.offsetValue !== null) {
                sql += ` OFFSET ${this.offsetValue}`;
            }
        } else if (this.offsetValue !== null) {
            sql += ` LIMIT 18446744073709551615 OFFSET ${this.offsetValue}`;
        }
        logDebug("最终生成的 SELECT SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

/* ============================================================
   导出模块
============================================================*/
module.exports = {
    MongoQueryBuilder,
    MongoAggregationBuilder,
    MongoUpdateBuilder,
    MongoDeleteBuilder,
    MongoInsertBuilder,
    SubQuery
};
