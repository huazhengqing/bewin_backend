
// npm install mysql

var mysql = require('mysql');

module.exports.handler = (event, context, callback) => {
     var connection = mysql.createConnection({
         host: <host-name>,
         user: <user-name>,
         password: <password>,
         database : <database-name>
     });
     connection.query('SELECT count(*) FROM `users`', (error, results, fields) => {
         if (error) 
            throw error;
         connection.end((err) => {
             callback(err, results);
         });
     });
 };