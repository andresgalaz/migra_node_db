var oConexionLocal = null,
    oConexionRemota = null,
    cDirAdjunto = null;
if (process.env.WSAPI_AMBIENTE == 'DESA') {
    console.log('Base desarrollo');
    cDirAdjunto = '/home/agalaz/adjunto';
    oConexionLocal = {
        host: '127.0.0.1', // your host
        port:3307,
        user: 'snapcar', // your database user
        password: 'snapcar', // your database password
        database: 'score',
        charset: 'UTF8_GENERAL_CI'
    };
    oConexionRemota = {
        host: '127.0.0.1', // your host
        user: 'snapcar', // your database user
        port:3307,
        password: 'snapcar', // your database password
        database: 'snapcar',
        charset: 'UTF8_GENERAL_CI'
    };
} else if (process.env.WSAPI_AMBIENTE == 'TEST') {
    console.log('Base test');
    cDirAdjunto = '/home/ubuntu/adjunto/';
    oConexionLocal = {
        host: '127.0.0.1', // your host
        user: 'snapcar', // your database user
        password: 'snapcar', // your database password
        database: 'score',
        charset: 'UTF8_GENERAL_CI'
    };
    oConexionRemota = {
        host: '127.0.0.1',
        user: 'snapcar',
        password: 'snapcar',
        database: 'snapcar',
        charset: 'UTF8_GENERAL_CI'
    };
} else if (process.env.WSAPI_AMBIENTE == 'PROD') {
    console.log('Base producción');
    cDirAdjunto = '/home/ubuntu/adjunto/';
    oConexionLocal = {
        host: '127.0.0.1', // your host
        user: 'snapcar', // your database user
        password: 'oycobe', // your database password
        database: 'score',
        charset: 'UTF8_GENERAL_CI'
    };
    oConexionRemota = {
        host: '127.0.0.1',
        // port: 23849,
        user: 'snapcar',
        password: 'oycobe',
        database: 'snapcar',
        charset: 'UTF8_GENERAL_CI'
    };
}

var dbLocal = require('knex')({
    client: 'mysql',
    connection: oConexionLocal
});
var dbRemota = require('knex')({
    client: 'mysql',
    connection: oConexionRemota
});

console.log(oConexionLocal, oConexionRemota);
module.exports = {
    'dbRemota': dbRemota,
    'dbLocal': dbLocal,
    'ambiente': process.env.WSAPI_AMBIENTE,
    'dirAdjunto': cDirAdjunto
};
