import pandas as pd
from flask import Flask, request, redirect, url_for, render_template
import pyodbc
from datetime import datetime
from decimal import Decimal, getcontext

app=Flask(__name__)

server_name = r'localhost\APPWEB'
database_name = 'MiniCore'
username = 'sa'
password = '12345'

def get_db_connection():
    conn = pyodbc.connect('Driver={SQL Server};'
                          f'Server={server_name};'
                          f'Database={database_name};'
                          f'UID={username};'
                          f'PWD={password};')
    return conn

def getDatos(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM MiniCore.dbo.Alumno
    """)
    
    results = cursor.fetchall()

    if results:
        for result in results:
            print(result)
    else:
        print("No hay resultados en la tabla P2")

    cursor.close()

@app.route('/cantidad_notas', methods=['GET', 'POST'])
def insert_cantidad_notas():
    if request.method == 'POST':
        cantidad_notas_p1 = request.form['cantidadNotasP1']
        cantidad_notas_p2 = request.form['cantidadNotasP2']
        cantidad_notas_p3 = request.form['cantidadNotasP3']

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Update P1 table
            cursor.execute('UPDATE MiniCore.dbo.P1 SET cantidadNotas = ? WHERE fechaInicio = ?', (cantidad_notas_p1, '2023-09-25'))
            
            # Update P2 table
            cursor.execute('UPDATE MiniCore.dbo.P2 SET cantidadNotas = ? WHERE fechaInicio = ?', (cantidad_notas_p2, '2023-10-19'))
            
            # Update P3 table
            cursor.execute('UPDATE MiniCore.dbo.P3 SET cantidadNotas = ? WHERE fechaInicio = ?', (cantidad_notas_p3, '2024-01-09'))

            conn.commit()

        except Exception as e:
            conn.rollback()
            print("Error al registrar la cantidad de notas", e)
            return "Error al registrar la cantidad de notas", 500
        finally:
            cursor.close()
            conn.close()

        return redirect(url_for('insert_cantidad_notas'))

    return render_template('cantidad_notas.html')

@app.route('/insert_alumno', methods=['GET', 'POST'])
def insert_alumno():
    if request.method == 'POST':
        id = request.form['id']
        nombre = request.form['nombre']

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Insertar en la tabla Alumno
            cursor.execute('INSERT INTO MiniCore.dbo.Alumno (id, nombre) VALUES (?, ?)', (id, nombre))
            
            conn.commit()

        except Exception as e:
            conn.rollback()
            print("Error al registrar el alumno", e)
            return "Error al registrar el alumno", 500
        finally:
            cursor.close()
            conn.close()

        return redirect(url_for('insert_alumno'))

    return render_template('insert_alumno.html')


@app.route('/insert_notas', methods=['GET', 'POST'])
def insert_notas():
    if request.method == 'POST':
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Obtén los datos del formulario
            id = request.form['id']
            nota = request.form['nota']
            fecha = datetime.strptime(request.form['fecha'], '%Y-%m-%d')


            # Validar que la fecha esté dentro de los intervalos permitidos
            fecha_inicio_p1 = datetime.strptime('2023-09-25', '%Y-%m-%d')
            fecha_fin_p1 = datetime.strptime('2023-10-18', '%Y-%m-%d')

            fecha_inicio_p2 = datetime.strptime('2023-10-19', '%Y-%m-%d')
            fecha_fin_p2 = datetime.strptime('2024-01-08', '%Y-%m-%d')

            fecha_inicio_p3 = datetime.strptime('2024-01-09', '%Y-%m-%d')
            fecha_fin_p3 = datetime.strptime('2024-02-08', '%Y-%m-%d')

            print("Fecha ingresada:", fecha.strftime('%Y-%m-%d'))
        
            # Ejecuta la consulta SQL para recuperar la cantidad de notas desde la tabla P1
            cursor.execute("SELECT [cantidadNotas] FROM [MiniCore].[dbo].[P1]")
            cantidad_notas_p1 = cursor.fetchone()[0]

            print("Cantidad de notas en P1:", cantidad_notas_p1)

            # Ejecuta la consulta SQL para recuperar la cantidad de notas desde la tabla P2
            cursor.execute("SELECT [cantidadNotas] FROM [MiniCore].[dbo].[P2]")
            cantidad_notas_p2 = cursor.fetchone()[0]

            print("Cantidad de notas en P2:", cantidad_notas_p2)

            # Ejecuta la consulta SQL para recuperar la cantidad de notas desde la tabla P3
            cursor.execute("SELECT [cantidadNotas] FROM [MiniCore].[dbo].[P3]")
            cantidad_notas_p3 = cursor.fetchone()[0]

            print("Cantidad de notas en P3:", cantidad_notas_p3)

            # Verificaciones para P1
            if fecha_inicio_p1 <= fecha <= fecha_fin_p1:
                print("La fecha está dentro del intervalo permitido para P1")

                # Ejecuta la consulta SQL para recuperar la cantidad de notas ingresadas hasta ahora
                consulta_sql = f"SELECT COUNT(*) FROM [MiniCore].[dbo].[Notas] WHERE [fecha] BETWEEN '2023-09-25' AND '2023-10-18'"
                cursor.execute(consulta_sql)
                cantidad_notas_ingresadas = cursor.fetchone()[0]
                print("Cantidad de notas ingresadas:", cantidad_notas_ingresadas)

                # Verifica si se ha excedido la cantidad de notas permitidas para P1
                if cantidad_notas_ingresadas < cantidad_notas_p1:  # Ajusta el límite según sea necesario
                    # Inserta los datos en la tabla Notas para P1
                    cursor.execute("INSERT INTO [MiniCore].[dbo].[Notas] ([id],[nota], [fecha]) VALUES (?, ?, ?)", id, nota, fecha)
                    conn.commit()
                else:
                    return "Se ha excedido la cantidad de notas permitidas para la fecha especificada", 500

            # Verificaciones para P2
            elif fecha_inicio_p2 <= fecha <= fecha_fin_p2:
                print("La fecha está dentro del intervalo permitido para P2")

                # Ejecuta la consulta SQL para recuperar la cantidad de notas ingresadas hasta ahora
                consulta_sql = f"SELECT COUNT(*) FROM [MiniCore].[dbo].[Notas] WHERE [fecha] BETWEEN '2023-10-19' AND '2024-01-08'"
                cursor.execute(consulta_sql)
                cantidad_notas_ingresadas2 = cursor.fetchone()[0]
                print("Cantidad de notas ingresadas:", cantidad_notas_ingresadas2)

                # Verifica si se ha excedido la cantidad de notas permitidas para P2
                if cantidad_notas_ingresadas2 < cantidad_notas_p2:  # Ajusta el límite según sea necesario
                    # Inserta los datos en la tabla Notas para P2
                    cursor.execute("INSERT INTO [MiniCore].[dbo].[Notas] ([id],[nota], [fecha]) VALUES (?, ?, ?)", id, nota, fecha)
                    conn.commit()
                else:
                    return "Se ha excedido la cantidad de notas permitidas para la fecha especificada", 500

            # Verificaciones para P3
            elif fecha_inicio_p3 <= fecha <= fecha_fin_p3:
                print("La fecha está dentro del intervalo permitido para P3")

                # Ejecuta la consulta SQL para recuperar la cantidad de notas ingresadas hasta ahora
                consulta_sql = f"SELECT COUNT(*) FROM [MiniCore].[dbo].[Notas] WHERE [fecha] BETWEEN '2024-01-09' AND '2024-02-08'"
                cursor.execute(consulta_sql)
                cantidad_notas_ingresadas3 = cursor.fetchone()[0]
                print("Cantidad de notas ingresadas:", cantidad_notas_ingresadas3)

                # Verifica si se ha excedido la cantidad de notas permitidas para P3
                if cantidad_notas_ingresadas3 < cantidad_notas_p3:  # Ajusta el límite según sea necesario
                    # Inserta los datos en la tabla Notas para P3
                    cursor.execute("INSERT INTO [MiniCore].[dbo].[Notas] ([id],[nota], [fecha]) VALUES (?, ?, ?)", id, nota, fecha)
                    conn.commit()
                else:
                    return "Se ha excedido la cantidad de notas permitidas para la fecha especificada", 500

            # Si ninguna de las condiciones anteriores es verdadera
            else:
                return "La fecha NO está dentro del intervalo permitido para P1, P2 ni P3", 500

        except ValueError as ve:
            conn.rollback()
            print("Error de validación:", ve)
            return str(ve), 400
        except Exception as e:
            conn.rollback()
            print("Error al registrar la cantidad de notas", e)
            return "Error al registrar la cantidad de notas", 500
        finally:
            cursor.close()
            conn.close()

        return redirect(url_for('insert_notas'))

    return render_template('insert_notas.html')


@app.route('/promedio_notas')
def promedio_notas():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Define los intervalos de fechas para cada período y sus factores de ponderación
    periodos = [
        {'nombre': 'P1', 'inicio': '2023-09-25', 'fin': '2023-10-18', 'factor': Decimal('2.5')},
        {'nombre': 'P2', 'inicio': '2023-10-19', 'fin': '2024-01-08', 'factor': Decimal('3.5')},
        {'nombre': 'P3', 'inicio': '2024-01-09', 'fin': '2024-02-08', 'factor': Decimal('4.0')},
    ]

    alumnos = {}
    for periodo in periodos:
        cursor.execute("""
        SELECT a.id, a.nombre, AVG(n.nota) as promedio
        FROM [MiniCore].[dbo].[Notas] n
        INNER JOIN [MiniCore].[dbo].[Alumno] a ON n.id = a.id
        WHERE n.fecha BETWEEN ? AND ?
        GROUP BY a.id, a.nombre
        """, (periodo['inicio'], periodo['fin']))

        for id, nombre, promedio in cursor.fetchall():
            if promedio is not None:
                promedio_ajustado = (Decimal(promedio) * periodo['factor']) / Decimal('10')
                if id not in alumnos:
                    alumnos[id] = {'nombre': nombre, 'promedios': {}, 'calculo_extra': None}
                alumnos[id]['promedios'][periodo['nombre']] = float(promedio_ajustado)
    
    # Realizar el cálculo extra para cada alumno
    for id, info in alumnos.items():
        if 'P1' in info['promedios'] and 'P2' in info['promedios']:
            info['calculo_extra'] = 6 - (info['promedios']['P1'] - info['promedios']['P2'])

    cursor.close()
    conn.close()

    return render_template('resultados.html', alumnos=alumnos.values())

@app.route('/')
def index():
    return redirect(url_for('insert_alumno'))

if __name__ == '__main__':
    app.run(debug=True)