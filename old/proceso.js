// Import dependencies
const _ = require('underscore-plus');
const moment = require('moment');
const batch = require('batchflow');

// Conexiones
var dbLocal = null;
var dbRemota = null;

dbLocal = require('knex')({
	client: 'mysql',
	connection: {
		host: '127.0.0.1',
		user: 'snapcar',
		password: 'snapcar',
		database: 'score',
		charset: 'UTF8_GENERAL_CI'
	}
});

dbRemota = require('knex')({
	client: 'mysql',
	connection: {
		host: '54.218.47.59',
		user: 'snapcar',
		port: 23849,
		password: 'oycobe',
		database: 'snapcar',
		charset: 'UTF8_GENERAL_CI'
	}
});

// Usuarioa a procesar, con equivalencias entre bases
var arrConvertUsuario = [{ idRemoto : 1  , idLocal: 28, fVehiculo: 57 } // Lucho
                        ,{ idRemoto : 7  , idLocal: 21, fVehiculo: 33 } // Gonza
                        ,{ idRemoto : 284, idLocal: 23, fVehiculo: 33 } // Yo veh Gonza
                        ,{ idRemoto : 266, idLocal: 18, fVehiculo: 33 } // Fabian
                        ,{ idRemoto : 7  , idLocal: 21, fVehiculo: 33 }
                        ,{ idRemoto : 284, idLocal: 23, fVehiculo: 57 }];

var arrEventos = [];
var nLastViaje = 1;

// Agrega las funciones a procesar en orden de proceso
var arrFunciones = [];

// Limpia eventos de los usuarios
arrFunciones[ arrFunciones.length ] = function(fnNext) {
	dbLocal('tEvento')
	// Convierte arreglo de objetos a un arreglo de int
	.whereIn( 'fUsuario', _.map( arrConvertUsuario, function(o){ return o.idLocal }))
	.del()
	.then(function(res){
		console.log('Se eliminó de tEvento:', res );
		fnNext();
	}).catch(function(e){
		fnNext(e);
	});
};

// Toma el ID del último viaje
arrFunciones[ arrFunciones.length ] = function(fnNext) {
	dbLocal('tEvento')
	.max('nIdViaje as nIdViaje')
	// Convierte arreglo de objetos a un arreglo de int
	.then(function(res){
		nLastViaje = res[0].nIdViaje + 1;
		console.log('Id. viaje inicial:', nLastViaje );
		fnNext();
	}).catch(function(e){
		fnNext(e);
	});
};

// Lee eventos desde la base remota
arrFunciones[ arrFunciones.length ] = function(fnNext) {
	dbRemota
	.select( 'id','prefix','puntos' ,'obs_value' ,'permited_value' ,'min_fecha' ,'max_fecha' ,'calle' )
	.from('xx_trip_observations')
	// Convierte arreglo de objetos a un arreglo de int
	.whereIn( 'id', _.map( arrConvertUsuario, function(o){ return o.idRemoto }))
	.orderBy( 'id' )
	.orderBy( 'min_fecha' )
	.then(function(data){
		var idUsrActual = -1;
		var idxIniViaje = -1;
		var idxLastEvento = -1;
		
		// Convierte datos de la pase remota a la base Local
		_.each( data, function( itm, idx, arr ){
			var evento = {
				nIdViaje         : nLastViaje,
				nIdTramo         : 1,
				tEvento          : itm.min_fecha,
				nLG              : 0,
				nLT              : 0,
				cCalle           : itm.calle,
				nVelocidadMaxima : itm.permited_value,
				nValor           : itm.obs_value,
				nPuntaje         : itm.puntos
			};
			// Cambia el tipo de evento de A,E y F a 3, 5 y 4, respectivamente
			evento.fTpEvento = [ 3, 4, 5 ][ _.indexOf(['A','E','F'], itm.prefix)];
			// Cambia del usuario remoto al local, y asigna vehiculo por defecto
			try {
				var oCnvUsr = _.find( arrConvertUsuario, function(o){ return o.idRemoto === itm.id; });
				evento.fUsuario = oCnvUsr.idLocal; 
				evento.fVehiculo = oCnvUsr.fVehiculo; 
			} catch( e ) {}

			// Lo agrega si está OK
			if( evento.fTpEvento && evento.fUsuario ){
				// Si cambió de conductor o pasaron mas de 10 minutos del último evento, es un nuevo viaje
 				var difMin = 0;
				if( idxLastEvento >= 0 ){
					difMin = moment(itm.min_fecha).diff(moment(arrEventos[idxLastEvento].tEvento),'minutes');
				}
				if( idUsrActual != itm.id || difMin > 10 ){
					idUsrActual = itm.id;
					if( idxLastEvento >= 0 ){
						// Crea fin del viaje del anterior
						var eventoFin = _.clone( arrEventos[idxLastEvento] );
						eventoFin.fTpEvento = 2;
						// Deja 4 minuos después
						eventoFin.tEvento = moment(eventoFin.tEvento).add({minutes:4}).toDate();
						// Calcula kilómetros a 0.9 KM por Minuto
						eventoFin.nValor = 0.9 * moment(arrEventos[idxLastEvento].tEvento).diff(moment(arrEventos[idxIniViaje].tEvento),'minutes');
						eventoFin.nPuntaje = 0;
						arrEventos.push(eventoFin);
					}
					// Incrementa viaje
					nLastViaje++;
					// Crea inicio viaje 4 minutos antes
					var eventoIni = _.clone( evento );
					eventoIni.nIdViaje = nLastViaje;
					eventoIni.fTpEvento = 1;
					// Deja 4 minuos antes
					eventoIni.tEvento = moment(eventoIni.tEvento).subtract({minutes:4}).toDate();
					eventoIni.nPuntaje = 0;
					idxIniViaje = arrEventos.length;
					arrEventos.push(eventoIni);

					// Cambia id viaje al evento nuevo
					evento.nIdViaje = nLastViaje;
				}
				idxLastEvento = arrEventos.length;
				arrEventos.push(evento);
				// lastFecha = itm.min_fecha;
			} else {
				// Evento fallido, falta información
				console.log( evento );
			}
		});
		// Cierra el ultimo viaje
		if( idxLastEvento >= 0 ){
			// Crea fin del viaje del anterior
			var eventoFin = _.clone( arrEventos[idxLastEvento] );
			eventoFin.fTpEvento = 2;
			// Deja 4 minuos después
			eventoFin.tEvento = moment(eventoFin.tEvento).add({minutes:4}).toDate();
			eventoFin.nValor = 0.9 * moment(arrEventos[idxLastEvento].tEvento).diff(moment(arrEventos[idxIniViaje].tEvento),'minutes');
			eventoFin.nPuntaje = 0;
			arrEventos.push(eventoFin);
		}
		fnNext();
	}).catch(function(e){
		fnNext(e);
	});
};


// Inserta resulta en la tabla tEvento
arrFunciones[ arrFunciones.length ] = function(fnNext) {
	console.log( 'Insertando ', arrEventos.length, ' registros');
	dbLocal.transaction( function(trx) {
		dbLocal('tEvento').transacting(trx).insert( arrEventos )
   		// .then(function(resp) { })
    	.then(trx.commit)
    	.catch(trx.rollback);

	}).then(function(resp) {
		console.log('Transaction complete.');
		fnNext();
	}).catch(function(err) {
		console.error(err);
	});
};

batch(arrFunciones).sequential()
	.each( function( i, item, fnNext ){
		console.log('Paso Nº ',i);
		item(fnNext);
	})
	.error(function(e){
		console.error('Salida con error');
		console.error(e);
		process.exit(0);
	})
	.end(function(){
		console.log('FIN');
		process.exit(0);
	});
