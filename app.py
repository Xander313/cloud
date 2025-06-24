from flask import Flask, request, jsonify
import pymysql
import requests
import json
from datetime import datetime, timedelta

app = Flask(__name__)


DB_CONFIG = {
    'host': 'MYSQLPHP',
    'user': 'root',
    'password': 'admin',
    'db': 'db_aguapotable',
    'port': 3306,
    'cursorclass': pymysql.cursors.DictCursor
}

import requests
import pymysql
from datetime import datetime, timedelta

def enviar_mensaje_telegram(chat_id):
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            cursor.execute("SELECT nameUser, id FROM usersWater WHERE chat_id = %s", (chat_id,))
            user = cursor.fetchone()
            if not user:
                return {'error': 'Usuario no encontrado'}

            nombre = user['nameUser']
            id_user = user['id']

            # Obtener consumo estático
            cursor.execute("""
                SELECT consumoEstatico 
                FROM lecturaEstatica 
                WHERE chat_id = %s 
                ORDER BY fechaCorte DESC LIMIT 1
            """, (chat_id,))
            row_estatico = cursor.fetchone()
            consumo_estatico = row_estatico['consumoEstatico'] if row_estatico else 0

            # Obtener total del mes actual
            cursor.execute("""
                SELECT SUM(consumoLitro) AS total_mes
                FROM lecturaDeterminacion
                WHERE chat_id = %s AND MONTH(fechaCorte) = MONTH(NOW()) AND YEAR(fechaCorte) = YEAR(NOW())
            """, (chat_id,))
            row_dinamico = cursor.fetchone()
            consumo_dinamico = row_dinamico['total_mes'] if row_dinamico and row_dinamico['total_mes'] else 0

            consumo_total = consumo_estatico + consumo_dinamico
            consumo_actual = consumo_dinamico

            now = datetime.utcnow() - timedelta(hours=5)
            fecha_envio = now.strftime("%d/%m/%Y %H:%M")

            if consumo_actual >= 3000:  # 10 m³
                mensaje = (
                    f"Hola <b>{nombre}</b>, el último corte ha dado los siguientes resultados:\n\n"
                    f"<b>Lectura al inicio de mes:</b> {consumo_estatico/1000:.2f} m³ \n"
                    f"<b>Última lectura:</b> {consumo_total/1000:.2f} m³ \n\n"
                    f"<b>⚠ADVERTENCIA⚠</b> Has superado el límite de consumo de agua mensual de 10m³.\n\n"
                    f"<b>Exceso de consumo:</b> {(consumo_dinamico-3000)/1000:.2f} m³ \n\n"
                    f"<b>A partir de ahora, cada metro cúbico tiene un recargo de $1 dólar.</b>\n\n"
                    f"<b>Corte al:</b> {fecha_envio}"
                )
            else:
                mensaje = (
                    f"Hola <b>{nombre}</b>, el último corte ha dado los siguientes resultados:\n\n"
                    f"<b>Lectura al inicio de mes:</b> {consumo_estatico/1000:.2f} m³ \n"
                    f"<b>Última lectura:</b> {consumo_total/1000:.2f} m³ \n\n"
                    f"<b>ℹ Consumo disponible:</b> {(3000-consumo_dinamico)/1000:.2f} m³ \n\n"
                    f"<b>Corte al:</b> {fecha_envio}"
                )

            # Enviar a Telegram
            TOKEN = "7992982183:AAH2kYLicJ5zM6NrAYExc_IowviLRJ723zo"
            url_telegram = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
            payload_telegram = {
                "chat_id": str(chat_id),
                "text": mensaje,
                "parse_mode": "HTML"
            }
            response_telegram = requests.post(url_telegram, data=payload_telegram)

            # Guardar notificación en DB
            cursor.execute("""
                INSERT INTO notification (chat_id, mensaje, estado)
                VALUES (%s, %s, %s)
            """, (chat_id, mensaje, response_telegram.status_code == 200))
            connection.commit()

            # Emitir lectura vía socket.io (Node.js)
            try:
                socket_data = {
                    "chat_id": chat_id,
                    "nombre": nombre,
                    "consumoLitro": consumo_dinamico,
                    "lecturaInicial": consumo_estatico,
                    "consumoTotal":consumo_total,
                    "dispositivo": "ESP32",
                    "fecha": fecha_envio
                }
                response_socket = requests.post("http://172.16.114.50:3000/emitir_lectura", json=socket_data)
                print("📡 Socket emitido:", response_socket.status_code, response_socket.text, flush=True)
                print("📤 Enviando al socket con los siguientes datos:")
                print(json.dumps(socket_data, indent=2), flush=True)
            except Exception as e:
                print("⚠️ Error al emitir por socket.io:", e, flush=True)

            if response_telegram.status_code == 200:
                return {'mensaje': 'Mensaje enviado correctamente'}
            else:
                return {'error': 'Error al enviar mensaje', 'telegram_response': response_telegram.text}

    except Exception as e:
        return {'error': 'Excepción en mensaje', 'detalle': str(e)}
    finally:
        if connection:
            connection.close()


@app.route('/mensaje', methods=['POST'])
def enviar_mensaje():
    data = request.get_json()
    chat_id = data.get("chat_id")
    if not chat_id:
        return jsonify({'error': 'Falta chat_id'}), 400

    resultado = enviar_mensaje_telegram(chat_id)
    return jsonify(resultado), 200 if "mensaje" in resultado else 500

@app.route('/guardar_lectura', methods=['POST'])
def guardar_lectura():
    connection = None

    try:
        data = request.get_json()
        print("📩 Datos recibidos del ESP32:", data, flush=True)

        chat_id = data.get("chat_id")
        consumo_litro = data.get("consumoLitro")

        if not chat_id or consumo_litro is None:
            print("❌ Faltan datos: chat_id o consumoLitro", flush=True)
            return jsonify({'error': 'Faltan datos chat_id o consumoLitro'}), 400

        # Abrir conexión
        connection = pymysql.connect(**DB_CONFIG)
        print("✅ Conexión a BD establecida", flush=True)

        with connection.cursor() as cursor:
            sql_user = "SELECT id FROM usersWater WHERE chat_id = %s"
            cursor.execute(sql_user, (chat_id,))
            user = cursor.fetchone()

            if not user:
                print(f"❌ Usuario no encontrado: chat_id={chat_id}", flush=True)
                return jsonify({'error': 'Usuario no encontrado'}), 404

            print(f"👤 Usuario encontrado: {user}", flush=True)

            sql_insert = """
                INSERT INTO lecturaDeterminacion (chat_id, consumoLitro, fechaCorte)
                VALUES (%s, %s, NOW())
            """
            cursor.execute(sql_insert, (chat_id, consumo_litro))
            connection.commit()
            print(f"✅ Lectura insertada para chat_id={chat_id}, consumo={consumo_litro}", flush=True)

        resultado = enviar_mensaje_telegram(chat_id)
        return jsonify({'mensaje': 'Lectura guardada', 'notificacion': resultado}), 200

    except Exception as e:
        print("💥 Excepción:", str(e), flush=True)
        return jsonify({'error': 'Error al guardar lectura', 'detalle': str(e)}), 500

    finally:
        if connection:
            try:
                connection.close()
                print("🔒 Conexión cerrada", flush=True)
            except:
                print("⚠️ Error al cerrar conexión", flush=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
