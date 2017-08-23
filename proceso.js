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
// Guarda los eventos a insertar
var arrEventosDeleted = [];
// ID del último viaje procesado
var tLastModif = null;

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
    dbRemota.select('id as trip_id', 'updated_at')
        .from('trips')
        .where('status', '=', 'S')
	.andWhere("from_date", '>', '2017-01-01')
        .orderBy('updated_at', 'desc')
        .limit(1).then(
        function(data) {
            console.log('UPDATED', data);
            tLastModif = data[0].updated_at;
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
            // Se toma desde el inicio del tiempo, antes del 2000 nada existía (en serio).
            tLastModif = new Date('2000-01-01 00:00:00')
            fnNext();
        } else if (data[0].tModif < tLastModif) {
            // Suma 3 horas, porque tenemos esa diferencia con la otra base
            tLastModif = data[0].tModif;
            console.log('Procesar ' + tLastModif);
            fnNext();
        } else
            console.assert(false, 'No hay nuevas actualizaciones');
    }).catch(function(e) {
        fnNext(e);
    });
};

// Lee eventos desde la base remota
arrFunciones[arrFunciones.length] = function(fnNext) {
    dbRemota.select('trip_id', 'vehicle_id', 'driver_id', 'observation_id', 'prefix', 'puntos', 'app_level', 'obs_value', 'permited_value',
            'obs_fecha', 'fecha_ini', 'fecha_fin', 'distance', 'calle', 'calle_corta',
            'calle_inicio', 'calle_inicio_corta', 'calle_fin', 'calle_fin_corta', 'latitude', 'longitude',
            'ts_modif').from('trip_observations_view')
        .where('ts_modif', '>=', tLastModif)
	.andWhere("fecha_ini", '>', '2017-01-01')
        .orderBy('trip_id').orderBy('obs_fecha').then(function(data) {
            var idTripActual = -1;
            var idxIniViaje = -1;

            // Convierte datos de la base remota a la base Local
            _.each(data, function(itm, idx, arr) {
                var evento = {
                    nIdViaje: itm.trip_id,
                    nIdTramo: 1,
                    fUsuario: parseInt(itm.driver_id),
                    fVehiculo: parseInt(itm.vehicle_id),
                    nIdObservation: itm.observation_id,
                    tEvento: fnHoraUtm_3menos(itm.obs_fecha),
                    nLG: itm.longitude,
                    nLT: itm.latitude,
                    cCalle: itm.calle,
                    cCalleCorta: itm.calle_corta,
                    nVelocidadMaxima: itm.permited_value,
                    nValor: itm.obs_value,
                    nPuntaje: itm.puntos,
                    nNivelApp: itm.app_level ? itm.app_level : 0,
                    tModif: itm.ts_modif
                };
                // Cambia el tipo de evento de A,E y F a 3, 5 y 4, respectivamente
                evento.fTpEvento = [3, 4, 5, 6, 5][_.indexOf(['A', 'F', 'E', 'C', 'X'], itm.prefix)];
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
                        eventoFin.cCalle = itm.calle_fin;
                        eventoFin.cCalleCorta = itm.calle_fin_corta;
                        arrEventos.push(eventoFin);
                    }
                    var eventoIni = _.clone(evento);
                    eventoIni.fTpEvento = 1;
                    eventoIni.tEvento = fnHoraUtm_3menos(itm.fecha_ini);
                    eventoIni.cCalle = itm.calle_inicio;
                    eventoIni.cCalleCorta = itm.calle_inicio_corta;

                    idxIniViaje = idx;
                    arrEventos.push(eventoIni);
                } else {
                    // Pone el MIN(tEvento) como evento del Inicio del viaje
                    if( idxIniViaje > 0 && data[idxIniViaje].tEvento > evento.tEvento )
                        data[idxIniViaje].tEvento = evento.tEvento;
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
                eventoFin.cCalle = itmIni.calle_fin;
                eventoFin.cCalleCorta = itmIni.calle_fin_corta;
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

// Lee observation eliminadas
arrFunciones[arrFunciones.length] = function(fnNext) {
    dbRemota.select('id', 'trip_id', 'prefix_observation', 'from_time', 'deleted_at').from('trip_observations_deleted_view').then(function(data) {
        arrEventosDeleted = data;
        fnNext();
    })
};

// limpia tabla temporal wEventoDeleted
arrFunciones[arrFunciones.length] = function(fnNext) {
    dbLocal.transaction(function(trx) {
        dbLocal('wEventoDeleted').transacting(trx).delete().then(trx.commit).catch(trx.rollback);
    }).then(function(resp) {
        console.log('Transaction complete.');
        fnNext();
    }).catch(function(err) {
        console.error('Borrando:', err.stack);
        console.log('Borrando:', err.message);
        fnNext(err);
    });
};

// Inserta resultados en la tabla temporal wEventoDeleted
arrFunciones[arrFunciones.length] = function(fnNext) {
    console.log('Insertando ', arrEventosDeleted.length, ' registros');
    dbLocal.transaction(function(trx) {
        dbLocal('wEventoDeleted').transacting(trx).insert(arrEventosDeleted).then(trx.commit).catch(trx.rollback);
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
