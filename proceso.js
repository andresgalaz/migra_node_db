// Import dependencies
const _ = require('underscore-plus');
const batch = require('batchflow');
const config = require('./config/main');
const moment = require('moment');

// Conexiones
var dbLocal = config.dbLocal;
var dbRemota = config.dbRemota;

// Guarda los eventos a insertar
var arrEventos = [];
// ID del último viaje procesado
var tLastModif = null;
var nLastViaje = 0;

// Agrega las funciones a procesar en orden de proceso
var arrFunciones = [];

/**
 * Ajusta fecha a Argentina/Buenos Aires -3
 * @param ts
 */
function fnHoraUtm_3(fecHora){
	var tOut = moment(fecHora, 'YYYY-MM-DD HH:mm:ss').subtract(3, 'h');
	if (tOut.isValid())
		return tOut.format('YYYY-MM-DD HH:mm:ss');
	return null;
}

// Toma el ID del último viaje acutalizado
arrFunciones[arrFunciones.length] = function(fnNext) {
	dbRemota.select('id as trip_id', 'updated_at').from('trips').orderBy('updated_at', 'desc').limit(1).then(
			function(data) {
				console.log('UPDATED', data);
				nLastViaje = data[0].trip_id;
				nLastModif = data[0].updated_at;
				fnNext();
			}).xcatch(function(e) {
		fnNext(e);
	});
};

// Verifica el ID del último viaje acutalizado
arrFunciones[arrFunciones.length] = function(fnNext) {
	dbLocal.select('nIdViaje', 'tModif').from('tEvento').orderBy('tModif', 'desc').limit(1).then(function(data) {
		console.log('UPDATED', data);
		if (data.length == 0) {
			console.log('Procesar Todo de nuevo');
			nLastViaje = 0;
			fnNext();
		} else if (data[0].tModif < nLastModif) {
			console.log('Procesar');
			fnNext();
		} else
			console.assert(false, 'No hay nuevas actualizaciones');
		// nLastViaje = data[0].trip_id
		// fnNext();
	}).xcatch(function(e) {
		fnNext(e);
	});
};

// Borra a partir del último viaje actualizado
arrFunciones[arrFunciones.length] = function(fnNext) {
	dbLocal('tEvento').where('nIdViaje', '>=', nLastViaje).del().then(function(data) {
		console.log('DELETE', data);
		fnNext();
	}).xcatch(function(e) {
		fnNext(e);
	});
};

// Lee eventos desde la base remota
arrFunciones[arrFunciones.length] = function(fnNext) {
	dbRemota.select('trip_id', 'vehicle_id', 'driver_id', 'prefix', 'puntos', 'obs_value', 'permited_value',
			'obs_fecha', 'fecha_ini', 'fecha_fin', 'distance', 'calle', 'calle_inicio', 'latitude', 'longitude',
			'ts_modif').from('trip_observations_view')
	// Convierte arreglo de objetos a un arreglo de int
	.where('trip_id', '>=', nLastViaje).orderBy('trip_id').orderBy('obs_fecha').then(function(data) {
		var idTripActual = -1;
		var idxIniViaje = -1;

		// Convierte datos de la base remota a la base Local
		_.each(data, function(itm, idx, arr) {
			var evento = {
				nIdViaje : itm.trip_id,
				nIdTramo : 1,
				fUsuario : parseInt(itm.driver_id),
				fVehiculo : parseInt(itm.vehicle_id),
				tEvento : fnHoraUtm_3( itm.obs_fecha ),
				nLG : itm.longitude,
				nLT : itm.latitude,
				cCalle : itm.calle,
				nVelocidadMaxima : itm.permited_value,
				nValor : itm.obs_value,
				nPuntaje : itm.puntos,
				tModif : itm.ts_modif
			};
			// Cambia el tipo de evento de A,E y F a 3, 5 y 4, respectivamente
			evento.fTpEvento = [ 3, 4, 5 ][_.indexOf([ 'A', 'F', 'E' ], itm.prefix)];

			// Lo agrega si está OK
			if (!isNaN(evento.fUsuario) && evento.fUsuario > 0 && !isNaN(evento.fVehiculo) && evento.fVehiculo > 0) {
				// Si hay camio de viaje, se cierra el anteior y se crea uno
				// nuevo
				if (idTripActual != itm.trip_id) {
					idTripActual = itm.trip_id;
					if (idxIniViaje >= 0) {
						// Crea fin del viaje del anterior, usa el último
						// evento como referencia
						var eventoFin = _.clone(_.last(arrEventos));
						var itmIni = data[idxIniViaje];
						eventoFin.fTpEvento = 2;
						eventoFin.tEvento = fnHoraUtm_3( itmIni.fecha_fin );
						eventoFin.nValor = itmIni.distance / 1000;
						eventoFin.nPuntaje = itmIni.puntos;
						eventoFin.nVelocidadMaxima = 0;
						arrEventos.push(eventoFin);
					}
					var eventoIni = _.clone(evento);
					eventoIni.fTpEvento = 1;
					eventoIni.tEvento = fnHoraUtm_3( itm.fecha_ini );
					eventoIni.cCalle = itm.calle_inicio;

					idxIniViaje = idx;
					arrEventos.push(eventoIni);
				}
				if (evento.fTpEvento)
					arrEventos.push(evento);
				// lastFecha = itm.obs_fecha;
			} else {
				// Evento fallido, falta información
				console.error('ńo usuario/vehiculo:', evento);
			}
			;
		});
		// Cierra el ultimo viaje
		if (idxIniViaje >= 0) {
			// Crea fin del viaje del anterior, usa el último evento como
			// referencia
			var eventoFin = _.clone(_.last(arrEventos));
			var itmIni = data[idxIniViaje];
			eventoFin.fTpEvento = 2;
			eventoFin.tEvento = fnHoraUtm_3( itmIni.fecha_fin );
			eventoFin.nValor = itmIni.distance / 1000;
			eventoFin.nPuntaje = itmIni.puntos;
			eventoFin.nVelocidadMaxima = 0;
			arrEventos.push(eventoFin);
		}
		fnNext();
	}).xcatch(function(e) {
		fnNext(e);
	});
};

// Inserta resultados en la tabla temporal wEvento
arrFunciones[arrFunciones.length] = function(fnNext) {
	console.log('Insertando ', arrEventos.length, ' registros');
	dbLocal.transaction(function(trx) {
		dbLocal('wEvento').transacting(trx).insert(arrEventos)
		.then(trx.commit).xcatch(trx.rollback);

	}).then(function(resp) {
		console.log('Transaction complete.');
		fnNext();
	}).xcatch(function(err) {
		console.error('Insertando:', err.stack);
		console.log(err.message);
		fnNext(err);
	});
};

batch(arrFunciones).sequential().each(function(i, item, fnNext) {
	console.log('Paso Nº ', i);
	item(fnNext);
}).error(function(e) {
	if (e.name == 'AssertionError') {
		console.log('FIN');
	} else {
		console.error('Proceso global:', e.stack);
		console.log(e.message);
	}
	process.exit(0);
}).end(function() {
	console.log('FIN');
	process.exit(0);
});
