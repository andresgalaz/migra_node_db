var oConexion = null, cDirAdjunto = null;
if (process.env.WSAPI_AMBIENTE == 'DESA') {
	cDirAdjunto = '/home/agalaz/adjunto';
	oConexion = {
		host : '127.0.0.1', // your host
		user : 'snapcar', // your database user
		password : 'snapcar', // your database password
		database : 'score_desa',
		charset : 'UTF8_GENERAL_CI'
	};
} else if (process.env.WSAPI_AMBIENTE == 'PROD') {
	cDirAdjunto = '/home/ubuntu/adjunto/';
	oConexion = {
		host : '127.0.0.1', // your host
		user : 'snapcar', // your database user
		password : 'snapcar', // your database password
		database : 'score',
		charset : 'UTF8_GENERAL_CI'
	};
}

var dbLocal = require('knex')({
	client : 'mysql',
	connection : oConexion
});
var dbRemota = require('knex')({
	client : 'mysql',
	connection : {
		host : 'data.appcar.com.ar',
		port : 23849,
		user : 'snapcar',
		password : 'oycobe',
		database : 'snapcar',
		charset : 'UTF8_GENERAL_CI'
	}
});

module.exports = {
	'dbRemota' : dbRemota,
	'dbLocal' : dbLocal,
	'ambiente' : process.env.WSAPI_AMBIENTE,
	'dirAdjunto' : cDirAdjunto
};
