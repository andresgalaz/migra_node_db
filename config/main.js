var dbLocal = require('knex')({
	client: 'mysql',
	connection: {
		host: '127.0.0.1',
		user: 'snapcar',
		password: 'snapcar',
		database: 'score_desa',
		charset: 'UTF8_GENERAL_CI'
	}
});
var dbRemota = require('knex')({
	client: 'mysql',
	connection: {
		host: '7puentes',
		user: 'snapcar',
		password: 'oycobe',
		database: 'snapcar',
		charset: 'UTF8_GENERAL_CI'
	}
});

module.exports = {
  'dbRemota': dbRemota,
  'dbLocal': dbLocal
};
