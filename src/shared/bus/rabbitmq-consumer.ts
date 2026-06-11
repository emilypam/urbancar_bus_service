import { getChannel, EXCHANGE } from './rabbitmq.js';
import { broadcast } from '../ws/socket-server.js';
import type { DomainEvent } from './event-types.js';

const ROUTING_TO_WS: Record<string, string> = {
  'reservas.reserva.creada':               'reserva:creada',
  'reservas.reserva.confirmada':           'reserva:confirmada',
  'reservas.reserva.cancelada':            'reserva:cancelada',
  'reservas.reserva.completada':           'reserva:completada',
  'pagos.pago.procesado':                  'pago:procesado',
  'pagos.pago.fallido':                    'pago:fallido',
  'facturas.factura.generada':             'factura:generada',
  'alquileres.alquiler.iniciado':          'alquiler:iniciado',
  'alquileres.devolucion.registrada':      'devolucion:registrada',
  'mantenimiento.vehiculo.en_mantenimiento': 'vehiculo:actualizado',
  'mantenimiento.vehiculo.disponible':     'vehiculo:actualizado',
  'mantenimiento.kardex.registrado':       'kardex:registrado',
};

export async function startConsumer(): Promise<void> {
  const ch = getChannel();
  if (!ch) {
    console.warn('[bus-consumer] canal no disponible — reintentando en 5s');
    setTimeout(startConsumer, 5000);
    return;
  }

  try {
    const { queue } = await ch.assertQueue('', { exclusive: true, autoDelete: true });
    await ch.bindQueue(queue, EXCHANGE, '#');

    console.log(`[bus-consumer] suscrito → exchange:${EXCHANGE} queue:${queue}`);

    ch.consume(queue, (msg) => {
      if (!msg) return;
      ch.ack(msg);
      try {
        const event: DomainEvent = JSON.parse(msg.content.toString());
        const wsEvt = ROUTING_TO_WS[event.eventType] ?? event.eventType;
        console.log(`[bus-consumer] ${event.eventType} → ws:'${wsEvt}'`);
        broadcast(wsEvt, event);
      } catch (err) {
        console.error('[bus-consumer] error parseando mensaje:', err);
      }
    });
  } catch (err) {
    console.error('[bus-consumer] error iniciando consumer:', err);
    setTimeout(startConsumer, 5000);
  }
}
