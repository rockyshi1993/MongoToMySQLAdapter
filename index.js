"use strict";

/* ============================================================
   配置与全局日志、错误、插件定义
============================================================ */
const Config = {
    LOG_LEVEL: process.env.LOG_LEVEL || "DEBUG"
};

const LOG_LEVELS = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };
let currentLogLevel = LOG_LEVELS[Config.LOG_LEVEL] || LOG_LEVELS.DEBUG;

/**
 * 日志管理器：统一处理不同日志级别的输出
 */
const Logger = {
    setLevel(levelStr) {
        currentLogLevel = LOG_LEVELS[levelStr] || LOG_LEVELS.DEBUG;
    },
    debug: (...args) => {
        if (currentLogLevel <= LOG_LEVELS.DEBUG) console.debug("[DEBUG]", ...args);
    },
    info: (...args) => {
        if (currentLogLevel <= LOG_LEVELS.INFO) console.info("[INFO]", ...args);
    },
    warn: (...args) => {
        if (currentLogLevel <= LOG_LEVELS.WARN) console.warn("[WARN]", ...args);
    },
    error: (...args) => {
        if (currentLogLevel <= LOG_LEVELS.ERROR) console.error("[ERROR]", ...args);
    }
};

/**
 * 简单日志封装函数
 */
function log(level, message, ...args) {
    if (currentLogLevel <= LOG_LEVELS[level]) {
        console[level](`[${level}]`, message, ...args);
    }
}

/**
 * 错误类型定义
 */
class QueryParseError extends Error {
    constructor(message, context) {
        super(`[QueryParseError] ${message} | Context: ${context}`);
        this.name = "QueryParseError";
    }
}

class SQLGenerationError extends Error {
    constructor(message, context) {
        super(`[SQLGenerationError] ${message} | Context: ${context}`);
        this.name = "SQLGenerationError";
    }
}

/**
 * 统一错误处理函数
 */
function handleError(errMsg, context, ErrorType) {
    Logger.error(`[${ErrorType.name}] ${errMsg} | Context: ${context}`);
    throw new ErrorType(errMsg, context);
}

/**
 * 操作符映射：MongoDB 与 MySQL 之间的对照
 */
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

/**
 * 插件扩展机制：支持自定义操作符转换
 */
const OperatorPlugins = []; // 改为数组存放插件对象

class OperatorPlugin {
    constructor(operator, handler) {
        this.operator = operator;
        this.handler = handler;
    }
    apply(field, opValue, conditions, params, context) {
        this.handler(field, opValue, conditions, params, context);
    }
}

function registerOperatorPlugin(operator, handler) {
    OperatorPlugins.push(new OperatorPlugin(operator, handler));
}

/* ============================================================
   公共 SQL 生成函数
============================================================ */
/**
 * 根据传入配置拼接 SQL 语句
 * @param {Object} queryConfig
 * @returns {{sql: string, params: Array}}
 */
function generateSQL(queryConfig) {
    let { selectClause, whereClause, joinClause, sortClause, limitClause, offsetClause, params, tableName } = queryConfig;
    let sql = `SELECT ${selectClause} FROM ${tableName}`;
    if (joinClause) sql += joinClause;
    if (whereClause) sql += ` WHERE ${whereClause}`;
    if (sortClause) sql += ` ORDER BY ${sortClause}`;
    if (limitClause) sql += ` LIMIT ${limitClause}`;
    if (offsetClause) sql += ` OFFSET ${offsetClause}`;
    return { sql, params };
}

/* ============================================================
   MongoDB 查询转换为 MySQL 查询函数
============================================================ */
function parseMongoQuery(query, params, context = "root") {
    let conditions = [];
    for (let key in query) {
        const value = query[key];
        try {
            if (key.startsWith('$')) {
                Logger.debug(`处理逻辑操作符 (${context}):`, key, value);
                handleLogicalOperators(key, value, conditions, params, context);
            } else {
                if (value instanceof SubQuery) {
                    Logger.debug(`子查询检测 (${context})，字段:`, key);
                    const subResult = value.toSQL();
                    conditions.push(`${key} IN ${subResult}`);
                    params.push(...value.getParams());
                } else if (typeof value === 'object' && !Array.isArray(value) && value !== null) {
                    let fieldConds = [];
                    for (let op in value) {
                        if (op.startsWith('$')) {
                            if (op === "$in" && value[op] instanceof SubQuery) {
                                Logger.debug(`子查询 in 操作符 (${context})，字段:`, key);
                                const subResult = value[op].toSQL();
                                fieldConds.push(`${key} IN ${subResult}`);
                                params.push(...value[op].getParams());
                            } else if (['$in', '$nin', '$all'].includes(op)) {
                                handleArrayOperators(key, value[op], op, fieldConds, params, context);
                            } else {
                                handleOperator(key, value[op], op, fieldConds, params, context);
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
            const errMsg = `Error processing key "${key}" in context "${context}": ${e.message}`;
            Logger.error(errMsg);
            throw new QueryParseError(e.message, context);
        }
    }
    const result = conditions.length ? conditions.join(" AND ") : "1=1";
    Logger.debug(`生成的 WHERE 子句 (${context}):`, result, "参数:", params);
    return result;
}

function handleLogicalOperators(operator, value, conditions, params, context) {
    if (!Array.isArray(value) || value.length === 0) {
        conditions.push(operator === '$and' ? "1=1" : "1=0");
        return;
    }
    let subConds = value.map((subQuery, idx) =>
        "(" + parseMongoQuery(subQuery, params, `${context}->${operator}[${idx}]`) + ")"
    );
    if (operator === '$nor') {
        conditions.push("NOT (" + subConds.join(" OR ") + ")");
    } else {
        conditions.push("(" + subConds.join(" " + (OPERATORS_MYSQL[operator] || operator) + " ") + ")");
    }
    Logger.debug(`逻辑操作符处理 (${context}) ${operator}:`, conditions[conditions.length - 1]);
}

function handleArrayOperators(field, values, operator, conditions, params, context) {
    if (!Array.isArray(values) || values.length === 0) {
        Logger.warn(`数组操作符 (${context}) ${operator} 的值为空，跳过字段 ${field}`);
        return;
    }
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
    Logger.debug(`数组操作符处理 (${context}) ${operator} for field ${field}:`, conditions[conditions.length - 1]);
}

function handleOperator(field, opValue, operator, conditions, params, context) {
    // 尝试使用插件处理
    for (const plugin of OperatorPlugins) {
        if (plugin.operator === operator) {
            plugin.apply(field, opValue, conditions, params, context);
            Logger.debug(`插件处理操作符 (${context}) ${operator} for field ${field}:`, conditions[conditions.length - 1]);
            return;
        }
    }
    if (opValue instanceof SubQuery) {
        const subSql = opValue.toSQL();
        conditions.push(`${field} IN ${subSql}`);
        params.push(...opValue.getParams());
        Logger.debug(`子查询处理 (${context}) for field ${field}:`, subSql);
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
                handleError(`Unsupported operator "${operator}" for field "${field}"`, context, SQLGenerationError);
            }
            conditions.push(`${field} ${sqlOperator} ?`);
            params.push(opValue);
    }
    Logger.debug(`操作符处理 (${context}) ${operator} for field ${field}:`, conditions[conditions.length - 1]);
}

/* ============================================================
   基本查询与 Join 查询生成函数
============================================================ */
/**
 * 基本的 SELECT 查询转换函数
 */
function mongoToMySQL(query, tableName, limit = 10, orderBy = 'id DESC') {
    let params = [];
    const whereClause = parseMongoQuery(query, params, "mongoToMySQL");
    const sql = `SELECT * FROM ${tableName} WHERE ${whereClause} ORDER BY ${orderBy} LIMIT ${limit}`;
    Logger.info("基本查询 SQL:", sql, "参数:", params);
    return { sql, params };
}

/**
 * 处理连表查询的辅助函数
 */
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
    Logger.debug("连表映射生成:", mappings);
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
    Logger.debug("生成的投影 SELECT 片段:", result);
    return result;
}

function generateJoinClause(joinConfigs) {
    const joinClause = joinConfigs.map(join => {
        const aliasPart = join.alias ? (' AS ' + join.alias) : '';
        return ` ${join.joinType} ${join.tableName}${aliasPart} ON ${join.on}`;
    }).join(" ");
    Logger.debug("生成的 JOIN 子句:", joinClause);
    return joinClause;
}

function mongoToMySQLWithJoinsOptimized(query, tableName, joinConfigs = [], limit = 10, orderBy = 'id DESC') {
    let params = [];
    const joinMappings = prepareJoinMappings(joinConfigs);
    let selectClause = "*";
    if (query.$project) {
        selectClause = parseProjectStageWithJoinsOptimized(query.$project, joinMappings, tableName);
        delete query.$project;
    }
    const whereClause = parseMongoQuery(query, params, "mongoToMySQLWithJoinsOptimized");
    const joinClause = generateJoinClause(joinConfigs);
    const sql = `SELECT ${selectClause} FROM ${tableName}${joinClause} WHERE ${whereClause} ORDER BY ${orderBy} LIMIT ${limit}`;
    Logger.info("连表查询 SQL:", sql, "参数:", params);
    return { sql, params };
}

/* ============================================================
   子查询支持
============================================================ */
class SubQuery {
    constructor(queryBuilder) {
        this.queryBuilder = queryBuilder;
        this.cachedSQL = null;
    }
    toSQL() {
        if (!this.cachedSQL) {
            const result = this.queryBuilder.toSQL();
            this.cachedSQL = "(" + result.sql + ")";
        }
        return this.cachedSQL;
    }
    getParams() {
        return this.queryBuilder.toSQL().params;
    }
}

/* ============================================================
   聚合查询支持
============================================================ */
function parseGroupStage(groupObj) {
    let selectParts = [];
    let groupByParts = [];
    let havingParts = []; // 暂未处理 HAVING
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
                handleError("不支持的聚合操作符: " + operator, "parseGroupStage", SQLGenerationError);
        }
        let operandStr = (typeof operand === "string" && operand.startsWith("$"))
            ? operand.substring(1)
            : operand;
        selectParts.push(`${sqlFunc}(${operandStr}) AS ${key}`);
    }
    return {
        selectClause: selectParts.join(", "),
        groupByClause: groupByParts.join(", "),
        havingClause: havingParts.join(" AND ")
    };
}

class MongoAggregationBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.pipeline = [];
        this.joinClause = ""; // 用于 $lookup 生成 JOIN 子句
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
    lookup({ from, localField, foreignField, as }) {
        this.pipeline.push({ $lookup: { from, localField, foreignField, as } });
        return this;
    }
    unwind(field) {
        this.pipeline.push({ $unwind: field });
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
        let comments = []; // 用于保存 $unwind 等阶段的注释

        let sql = "SELECT " + selectClause + " FROM " + this.tableName;

        for (let stage of this.pipeline) {
            if (stage.$match) {
                whereClause += parseMongoQuery(stage.$match, params, "$match");
            } else if (stage.$group) {
                const groupResult = parseGroupStage(stage.$group);
                selectClause = groupResult.selectClause;
                groupClause = groupResult.groupByClause;
                havingClause = groupResult.havingClause;
            } else if (stage.$sort) {
                let sortArr = [];
                for (let key in stage.$sort) {
                    let direction = stage.$sort[key] === -1 ? "DESC" : "ASC";
                    sortArr.push(`${key} ${direction}`);
                }
                orderClause = sortArr.join(", ");
            } else if (stage.$limit) {
                limitClause = stage.$limit;
            } else if (stage.$skip) {
                offsetClause = stage.$skip;
            } else if (stage.$lookup) {
                const { from, localField, foreignField, as } = stage.$lookup;
                this.joinClause += ` LEFT JOIN ${from} AS ${as} ON ${this.tableName}.${localField} = ${as}.${foreignField}`;
            } else if (stage.$unwind) {
                comments.push(`/* UNWIND(${stage.$unwind}) */`);
            }
        }

        if (groupClause) {
            sql = "SELECT " + selectClause + " FROM " + this.tableName;
        }

        if (this.joinClause) {
            sql += this.joinClause;
        }
        if (comments.length > 0) {
            sql += " " + comments.join(" ");
        }
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

        Logger.info("生成的 SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

/* ============================================================
   更新、删除与新增支持
============================================================ */
class MongoUpdateBuilder {
    constructor(tableName, idField = "id") {
        this.tableName = tableName;
        this.idField = idField;
        this.dataObj = null;
        this.dataArray = null;
        this.filter = {};
        this.singleUpdate = false;
    }
    update(updateData) {
        if (Array.isArray(updateData)) {
            updateData.forEach(obj => {
                if (!obj.hasOwnProperty(this.idField)) {
                    handleError(`每个更新对象必须包含 '${this.idField}' 字段`, "MongoUpdateBuilder", SQLGenerationError);
                }
            });
            this.dataArray = updateData;
            this.dataObj = null;
        } else if (typeof updateData === 'object' && updateData !== null) {
            if (Object.keys(updateData).every(key => !key.startsWith('$'))) {
                this.dataObj = { $set: updateData };
            } else {
                this.dataObj = updateData;
            }
            this.dataArray = null;
        } else {
            handleError("更新数据必须为对象或数组。", "MongoUpdateBuilder", SQLGenerationError);
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
            handleError("更新操作必须指定查询条件，防止误更新所有记录。", "MongoUpdateBuilder", SQLGenerationError);
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
                        handleError("不支持的更新操作符: " + op, "MongoUpdateBuilder", SQLGenerationError);
                }
            }
            if (setClauses.length === 0) {
                handleError("未指定更新字段。", "MongoUpdateBuilder", SQLGenerationError);
            }
            sql = `UPDATE ${this.tableName} SET ${setClauses.join(", ")}`;
            let whereClause = parseMongoQuery(this.filter, params, "UPDATE");
            sql += ` WHERE ${whereClause}`;
        } else if (this.dataArray) {
            if (this.dataArray.length === 0) {
                handleError("未指定更新对象。", "MongoUpdateBuilder", SQLGenerationError);
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
            let extraCondition = parseMongoQuery(this.filter, extraParams, "BULK_UPDATE");
            if (extraCondition !== "1=1") {
                bulkWhereClause += ` AND (${extraCondition})`;
            }
            sql = `UPDATE ${this.tableName} SET ${setClauses.join(", ")} WHERE ${bulkWhereClause}`;
            params = caseParams.concat(ids, extraParams);
        } else {
            handleError("未指定更新数据。", "MongoUpdateBuilder", SQLGenerationError);
        }
        if (this.singleUpdate) {
            sql += " LIMIT 1";
        }
        Logger.info("生成的 UPDATE SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

class MongoDeleteBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.filter = {};
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
            handleError("删除操作必须指定查询条件，防止误删除所有记录。", "MongoDeleteBuilder", SQLGenerationError);
        }
        let params = [];
        let whereClause = parseMongoQuery(this.filter, params, "DELETE");
        let sql = `DELETE FROM ${this.tableName} WHERE ${whereClause}`;
        if (this.singleDelete) {
            sql += " LIMIT 1";
        }
        Logger.info("生成的 DELETE SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

class MongoInsertBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.documents = [];
    }

    // 插入一条记录
    insertOne(doc) {
        // 检查文档中的字段，处理 JSON 数据类型
        for (let key in doc) {
            if (typeof doc[key] === 'object' && doc[key] !== null) {
                // 对象或者数组，转换为 JSON 字符串
                doc[key] = JSON.stringify(doc[key]);
            }
        }
        this.documents.push(doc);
        return this;
    }

    // 插入多条记录
    insertMany(docs) {
        docs.forEach(doc => {
            for (let key in doc) {
                if (typeof doc[key] === 'object' && doc[key] !== null) {
                    // 对象或者数组，转换为 JSON 字符串
                    doc[key] = JSON.stringify(doc[key]);
                }
            }
        });
        this.documents = [...this.documents, ...docs];
        return this;
    }

    // 转换为 SQL
    toSQL() {
        if (this.documents.length === 0) {
            throw new SQLGenerationError('没有数据进行插入', 'MongoInsertBuilder');
        }

        const columns = Object.keys(this.documents[0]);
        const values = this.documents.map(doc => {
            return columns.map(column => {
                return doc[column];
            });
        });

        const sql = `INSERT INTO ${this.tableName} (${columns.join(', ')}) VALUES ${values
            .map(value => `(${value.map(v => (v === null ? 'NULL' : `'${v}'`)).join(', ')})`)
            .join(', ')}`;

        const params = this.documents.flatMap(doc => {
            return columns.map(column => doc[column]);
        });

        return { sql, params };
    }
}

/* ============================================================
   MongoQueryBuilder 类：支持链式调用构造查询
============================================================ */
class MongoQueryBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.filter = {};
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

    skip(skip) {
        this.offsetValue = skip;
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

        // 处理 JOIN 子句
        if (this.joinConfigs && this.joinConfigs.length > 0) {
            sql += generateJoinClause(this.joinConfigs);
        }

        // 处理 WHERE 子句
        const whereClause = parseMongoQuery(this.filter, params, "SELECT");
        if (whereClause) {
            sql += ` WHERE ${whereClause}`;
        }

        // 处理排序
        if (this.sortClause) {
            sql += ` ORDER BY ${this.sortClause}`;
        }

        // 处理分页
        if (this.limitValue !== null) {
            sql += ` LIMIT ${this.limitValue}`;
            if (this.offsetValue !== null) {
                sql += ` OFFSET ${this.offsetValue}`;
            }
        } else if (this.offsetValue !== null) {
            sql += ` LIMIT 18446744073709551615 OFFSET ${this.offsetValue}`;  // 设置大范围限制
        }

        Logger.info("最终生成的 SELECT SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

/* ============================================================
   模块导出
============================================================ */
module.exports = {
    MongoQueryBuilder,
    MongoAggregationBuilder,
    MongoUpdateBuilder,
    MongoDeleteBuilder,
    MongoInsertBuilder,
    SubQuery,
    registerOperatorPlugin,
    Logger,
    QueryParseError,
    SQLGenerationError,
    mongoToMySQL,
    mongoToMySQLWithJoinsOptimized
};
