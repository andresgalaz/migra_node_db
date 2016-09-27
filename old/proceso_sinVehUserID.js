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
		host: 'data.appcar.com.ar',
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
                        ,{ idRemoto : 280, idLocal: 23, fVehiculo: 33 } // Yo veh Gonza
                        ,{ idRemoto : 283, idLocal: 18, fVehiculo: 41 } // Fabian
                        ,{ idRemoto : 266, idLocal: 21, fVehiculo: 41 } // Gonza veh Fabian
                        ,{ idRemoto : 265, idLocal: 23, fVehiculo: 46 } // Yo veh Fabian 2
						];
arrConvertUsuario = [
                     { idRemoto : 7  , idLocal: 18, fVehiculo: 41 } // Fabi
                    ,{ idRemoto : 280, idLocal: 23, fVehiculo: 46 } // Yo veh Fabi 2
					];

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
	.select( 'trip_id','client_id','prefix','puntos' ,'obs_value' ,'permited_value' ,'obs_fecha' 
		   , 'fecha_ini', 'fecha_fin', 'distance', 'calle', 'calle_inicio', 'latitude', 'longitude' )
	.from('trip_observations_view')
	// Convierte arreglo de objetos a un arreglo de int
	.whereIn( 'client_id', _.map( arrConvertUsuario, function(o){ return o.idRemoto }))
	.orderBy( 'trip_id' )
	.orderBy( 'obs_fecha' )
	.then(function(data){
		var idTripActual = -1;
		var idxIniViaje = -1;
		
		// Convierte datos de la base remota a la base Local
		_.each( data, function( itm, idx, arr ){
			var evento = {
				nIdViaje         : nLastViaje,
				nIdTramo         : 1,
				tEvento          : itm.obs_fecha,
				nLG              : itm.longitude,
				nLT              : itm.latitude,
				cCalle           : itm.calle,
				nVelocidadMaxima : itm.permited_value,
				nValor           : itm.obs_value,
				nPuntaje         : itm.puntos
			};
			// Cambia el tipo de evento de A,E y F a 3, 5 y 4, respectivamente
			evento.fTpEvento = [ 3, 4, 5 ][ _.indexOf(['A','E','F'], itm.prefix)];
			// Cambia del usuario remoto al local, y asigna vehiculo por defecto
			try {
				var oCnvUsr = _.find( arrConvertUsuario, function(o){ return o.idRemoto === itm.client_id; });
				evento.fUsuario = oCnvUsr.idLocal; 
				evento.fVehiculo = oCnvUsr.fVehiculo; 
			} catch( e ) {}

			// Lo agrega si está OK
			if( evento.fTpEvento && evento.fUsuario ){
				// Si hay camio de viaje, se cierra el anteior y se crea uno nuevo
				if( idTripActual != itm.trip_id ){
					idTripActual = itm.trip_id;
					if( idxIniViaje >= 0 ){
						// Crea fin del viaje del anterior, usa el último evento como referencia
						var eventoFin = _.clone( _.last( arrEventos ));
						var itmIni = data[idxIniViaje];
						eventoFin.fTpEvento = 2;
						eventoFin.tEvento = itmIni.fecha_fin;
						eventoFin.nValor = itmIni.distance;
						eventoFin.nPuntaje = itmIni.puntos;
						eventoFin.nVelocidadMaxima = 0;
						arrEventos.push(eventoFin);
					}
					var eventoIni = _.clone( evento );
					eventoIni.fTpEvento	 = 1;
					eventoIni.tEvento    = itm.fecha_ini;
					eventoIni.cCalle     = itm.calle_inicio;

					idxIniViaje = idx;
					arrEventos.push(eventoIni);

					// Incrementa viaje
					nLastViaje++;
					evento.nIdViaje = nLastViaje;
				}
				arrEventos.push(evento);
				// lastFecha = itm.obs_fecha;
			} else {
				// Evento fallido, falta información
				console.log( evento );
			}
		});
		// Cierra el ultimo viaje
		if( idxIniViaje >= 0 ){
			// Crea fin del viaje del anterior, usa el último evento como referencia
			var eventoFin = _.clone( _.last( arrEventos ));
			var itmIni = data[idxIniViaje];
			eventoFin.fTpEvento = 2;
			eventoFin.tEvento = itmIni.fecha_fin;
			eventoFin.nValor = itmIni.distance;
			eventoFin.nPuntaje = itmIni.puntos;
			eventoFin.nVelocidadMaxima = 0;
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
