const mysql = require('mysql2/promise');
const Insert = require('./insert');
(async ()=>{
    async function connectToDatabase() {
        try {
            // 创建一个连接池（可以根据需要调整配置）
            const connection = await mysql.createConnection({
                host: 'localhost',        // 数据库地址
                user: 'root',             // 数据库用户名
                password: '',     // 数据库密码
                database: 'my_database',      // 你要连接的数据库名称
            });


            console.log('Successfully connected to the database.');
            const table = new Insert('test');
            const {sql,params} = await table.insertOne({name:'test',age:4,day:123},{upsert:true,fields:['age']});
            const res = await connection.execute(sql,params);
            console.log('Data from users table:', res);

            // 关闭连接
            await connection.end();

        } catch (error) {
            console.error('Error connecting to the database:', error);
        }
    }

    await connectToDatabase();
})()

