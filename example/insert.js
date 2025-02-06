const { MongoInsertBuilder } = require('../lib');
class crud {
    constructor(tableName) {
        this.table = new MongoInsertBuilder(tableName);
    }

    /**
     * 根据命中唯一索引判断新增还是更新
     * @param sql Sql
     * @param options Object
     * @returns {Promise<*>}
     */
    async upsert(sql,options={}){
        const {upsert,fields} = options;
        if(typeof options == "object" && options.upsert){
            sql = sql.upsert(upsert,fields);
        }
        return sql.toSQL();
    }

    /**
     * 单个插入
     * @param data Object
     * @param options Object
     * @returns {Promise<*>}
     */
    async insertOne(data,options={}) {
        try{
            let sql = this.table.insertOne(data);
            return await this.upsert(sql, options);
        }catch (error) {
            throw error;
        }
    }
}

module.exports = crud;