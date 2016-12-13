// Import dependencies
const _ = require('underscore-plus'), //
batch = require('batchflow'), //
config = require('./config/main'), //
moment = require('moment');

// Conexiones
var dbLocal = config.dbLocal;
var dbRemota = config.dbRemota;

// Guarda los eventos a insertar
var arrEventos = [];
// ID del último viaje procesado
var tLastModif = null;
// var nLastViaje = 0;

// Agrega las funciones a procesar en orden de proceso
var arrFunciones = [];

/**
 * Ajusta fecha a Argentina/Buenos Aires -3
 * 
 * @param ts
 */
function fnHoraUtm_3menos(fecHora) {
	var tOut = moment(fecHora, 'YYYY-MM-DD HH:mm:ss').subtract(3, 'h');
	if (tOut.isValid())
		return tOut.format('YYYY-MM-DD HH:mm:ss');
	return null;
}

function fnHoraUtm_3mas(fecHora) {
	var tOut = moment(fecHora, 'YYYY-MM-DD HH:mm:ss').add(3, 'h');
	if (tOut.isValid())
		return tOut.format('YYYY-MM-DD HH:mm:ss');
	return null;
}


// Toma el ID del último viaje actualizado
arrFunciones[arrFunciones.length] = function(fnNext) {
	dbRemota.select('id as trip_id', 'updated_at').from('trips').orderBy('updated_at', 'desc').limit(1).then(
			function(data) {
				console.log('UPDATED', data);
				// nLastViaje = data[0].trip_id;
				nLastModif = data[0].updated_at;
				fnNext();
			}).catch(function(e) {
		fnNext(e);
	});
};

// Verifica el ID del último viaje acutalizado
arrFunciones[arrFunciones.length] = function(fnNext) {
	dbLocal.select('nIdViaje', 'tModif').from('tEvento').orderBy('tModif', 'desc').limit(1).then(function(data) {
		console.log('UPDATED', data);
		if (data.length == 0) {
			console.log('Procesar Todo de nuevo');
			// nLastViaje = 0;
			// Se toma desde el inicio del tiempo, antes del 2000 nada existía (en serio).
			nLastModif = new Date('2000-01-01 00:00:00')
			fnNext();
		} else if (data[0].tModif < nLastModif) {
			// Suma 3 horas, porque tenemos esa diferencia con la otra base
			nLastModif = fnHoraUtm_3mas( data[0].tModif );
			console.log('Procesar ' + nLastModif);
			fnNext();
		} else
			console.assert(false, 'No hay nuevas actualizaciones');
	}).catch(function(e) {
		fnNext(e);
	});
};

/* Esto se comento, porque el que borra es el procedure prMigraEventos
// Borra a partir del último viaje actualizado
arrFunciones[arrFunciones.length] = function(fnNext) {
	dbLocal('tEvento').where('nIdViaje', '>=', nLastViaje).del().then(function(data) {
		console.log('DELETE', data);
		fnNext();
	}).catch(function(e) {
		fnNext(e);
	});
};
*/

// Lee eventos desde la base remota
arrFunciones[arrFunciones.length] = function(fnNext) {
	dbRemota.select('trip_id', 'vehicle_id', 'driver_id', 'prefix', 'puntos', 'obs_value', 'permited_value',
			'obs_fecha', 'fecha_ini', 'fecha_fin', 'distance', 'calle', 'calle_inicio', 'latitude', 'longitude',
			'ts_modif').from('trip_observations_view')
	// Convierte arreglo de objetos a un arreglo de int
	// .where('trip_id', '>=', nLastViaje)
	.where('ts_modif', '>=', nLastModif)
	.orderBy('trip_id').orderBy('obs_fecha').then(function(data) {
		var idTripActual = -1;
		var idxIniViaje = -1;

		// Convierte datos de la base remota a la base Local
		_.each(data, function(itm, idx, arr) {
			var evento = {
				nIdViaje : itm.trip_id,
				nIdTramo : 1,
				fUsuario : parseInt(itm.driver_id),
				fVehiculo : parseInt(itm.vehicle_id),
				tEvento : fnHoraUtm_3menos(itm.obs_fecha),
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
			// Corrige valores inválidos
			evento.fUsuario = (isNaN(evento.fUsuario) ? null : evento.fUsuario);
			evento.fVehiculo = (isNaN(evento.fVehiculo) ? null : evento.fVehiculo);

			// Si hay camio de viaje, se cierra el anteior y se crea uno nuevo
			if (idTripActual != itm.trip_id) {
				idTripActual = itm.trip_id;
				if (idxIniViaje >= 0) {
					// Crea fin del viaje del anterior, usa el último
					// evento como referencia
					var eventoFin = _.clone(_.last(arrEventos));
					var itmIni = data[idxIniViaje];
					eventoFin.fTpEvento = 2;
					eventoFin.tEvento = fnHoraUtm_3menos(itmIni.fecha_fin ? itmIni.fecha_fin : itmIni.fecha_ini);
					eventoFin.nValor = itmIni.distance / 1000;
					eventoFin.nPuntaje = itmIni.puntos;
					eventoFin.nVelocidadMaxima = 0;
					arrEventos.push(eventoFin);
				}
				var eventoIni = _.clone(evento);
				eventoIni.fTpEvento = 1;
				eventoIni.tEvento = fnHoraUtm_3menos(itm.fecha_ini);
				eventoIni.cCalle = itm.calle_inicio;

				idxIniViaje = idx;
				arrEventos.push(eventoIni);
			}
			
			if (evento.fTpEvento)
				arrEventos.push(evento);
		});
		// Cierra el ultimo viaje
		if (idxIniViaje >= 0) {
			// Crea fin del viaje del anterior, usa el último evento como
			// referencia
			var eventoFin = _.clone(_.last(arrEventos));
			var itmIni = data[idxIniViaje];
			eventoFin.fTpEvento = 2;
			eventoFin.tEvento = fnHoraUtm_3menos(itmIni.fecha_fin ? itmIni.fecha_fin : itmIni.fecha_ini);
			eventoFin.nValor = itmIni.distance / 1000;
			eventoFin.nPuntaje = itmIni.puntos;
			eventoFin.nVelocidadMaxima = 0;
			arrEventos.push(eventoFin);
		}
		fnNext();
	}).catch(function(e) {
		fnNext(e);
	});
};

// Inserta resultados en la tabla temporal wEvento
arrFunciones[arrFunciones.length] = function(fnNext) {
	console.log('Insertando ', arrEventos.length, ' registros');
	dbLocal.transaction(function(trx) {
		dbLocal('wEvento').transacting(trx).insert(arrEventos).then(trx.commit).catch(trx.rollback);
	}).then(function(resp) {
		console.log('Transaction complete.');
		fnNext();
	}).catch(function(err) {
		console.error('Insertando:', err.stack);
		console.log('Insertando:', err.message);
		fnNext(err);
	});
};

// Pasa los valores de wEvento a tEvento y limpia los procesados
arrFunciones[arrFunciones.length] = function(fnNext) {
	console.log('Ejecutando prMigraEventos');
	dbLocal.raw('call prMigraEventos') //
	.then(function(resp) {
		console.log('prMigraEventos terminado OK', resp);
		fnNext();
	}).catch(function(err) {
		console.error('prMigraEventos:', err);
		console.log('prMigraEventos:', err.message);
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
		console.log('Proceso global:', e.message);
	}
	process.exit(0);
}).end(function() {
	console.log('FIN');
	process.exit(0);
});
