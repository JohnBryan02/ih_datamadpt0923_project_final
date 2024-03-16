import pandas as pd
import numpy as np
#este se usara para guardar fechas con la semana actual
import datetime

#esto se usaran para enviar emails
from dotenv import dotenv_values
from email.message import EmailMessage
import ssl
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#import os

##DATASETS

#leemos los datasets
deliveries = pd.read_csv('data/deliveries.csv')
shifts = pd.read_csv('data/shifts.csv')
performance = pd.read_csv('data/performance2.csv')
justificaciones = pd.read_csv('data/justificaciones.csv')

##EDA

#Sacamos las medias frente a sus pedidos
performance['E2E Invoiced'] = performance['e2e_invoiced']/performance['deliveries_invoiced']/60
performance['E2E Succeeded'] = performance['e2e_succeeded']/performance['deliveries_succeeded']/60
performance['PACE Succeded'] = (performance['e2e_succeeded']/60)/((performance['distance_to_pu_succeeded']+performance['distance_to_do_succeeded'])/1000)
performance['WAPU'] = performance['wapu']/performance['performance_deliveries']/60
performance['WADO'] = performance['wado']/performance['performance_deliveries']/60
performance['PACE PU Succeded'] = (performance['picking_time'] / 
                                   (performance['picking_time'] + 
                                    performance['wapu'] + 
                                    performance['delivering_time'] + 
                                    performance['wado'])) * ((performance['e2e_succeeded'] / 
                                                              performance['deliveries_succeeded']) / 60) / ((performance['distance_to_pu_succeeded'] / 
                                                                                                             1000) / performance['deliveries_succeeded'])
performance['PACE DO Succeded'] = (performance['delivering_time'] / 
                                   (performance['picking_time'] + performance['wapu'] + 
                                    performance['delivering_time'] + 
                                    performance['wado'])) * ((performance['e2e_succeeded'] / 
                                                              performance['deliveries_succeeded']) / 60) / ((performance['distance_to_do_succeeded'] / 
                                                                                                             1000) / performance['deliveries_succeeded'])
performance['PACE PU vs DO'] = performance['PACE PU Succeded'] - performance['PACE DO Succeded']
performance['Connection Rate'] = performance['online_hours'] / performance['assigned_hours']
performance['Deliveries'] = performance['deliveries_invoiced']
performance['Online'] = performance['online_hours']
performance['Online Rate'] = np.where(performance['assigned_hours'] != 0,
                                performance['Online'] / performance['assigned_hours'],
                                0)
performance['Del/h'] = np.where(performance['Online'] != 0, performance['Deliveries'] / performance['Online'], 0)
performance['Cancelations'] = performance['non_commit'] / performance['invitations']
performance['IAB'] = performance['is_in_area_begin'] / performance['assigned_shifts']


#Seleccionamos las columnas que nos interesan
columnas = ['driver_id','week','transport_type','E2E Invoiced','E2E Succeeded','PACE Succeded','WAPU','WADO','PACE PU Succeded','PACE DO Succeded','PACE PU vs DO','Connection Rate','Deliveries','Online','Online Rate','Del/h','Cancelations','IAB']
#Cremaos un nuevo df con las columnas que nos interesan.
transformed_performance = performance.loc[:, columnas]
#Seleccionamos las columnas númericas que para poder hacer un medía y agruparlas por driver_id
columnas_numericas = ['WAPU','WADO','PACE PU Succeded','PACE DO Succeded','PACE PU vs DO','Online Rate','Cancelations','IAB']
#Hacemos el group by y indicamos que nos haga las medías de las columnas númericas.
new_df = transformed_performance.groupby(['driver_id','transport_type'])[columnas_numericas].mean().reset_index()

#Normalizamos y sacamos nuevas columnas con el scooring de las columnas que hemos seleccionado
new_df['score_WAPU'] = (new_df['WAPU'] - new_df['WAPU'].min()) / (new_df['WAPU'].max() - new_df['WAPU'].min())
new_df['score_WADO'] = (new_df['WADO'] - new_df['WADO'].min()) / (new_df['WADO'].max() - new_df['WADO'].min())
new_df['score_PACE PU Succeded'] = (new_df['PACE PU Succeded'] - new_df['PACE PU Succeded'].min()) / (new_df['PACE PU Succeded'].max() - new_df['PACE PU Succeded'].min())
new_df['score_PACE DO Succeded'] = (new_df['PACE DO Succeded'] - new_df['PACE DO Succeded'].min()) / (new_df['PACE DO Succeded'].max() - new_df['PACE DO Succeded'].min())
new_df['score_PACE PU vs DO'] = (new_df['PACE PU vs DO'] - new_df['PACE PU vs DO'].min()) / (new_df['PACE PU vs DO'].max() - new_df['PACE PU vs DO'].min())
new_df['score_Online Rate'] = new_df['Online Rate']
new_df['score_Cancelations'] = new_df['Cancelations']
new_df['score_IAB'] = new_df['IAB']

#Sacamos un único scooring sumando las positivas y restando las que són negativas.
new_df['Total_Score'] = -new_df['score_WAPU']-new_df['score_WADO']-new_df['score_PACE PU Succeded']-new_df['score_PACE DO Succeded']-new_df['score_PACE PU vs DO']+new_df['score_Online Rate']-new_df['score_Cancelations']+new_df['score_IAB']

#Generamos el ranking para ver quienes son los tops.
final_df = new_df.sort_values(by='Total_Score',ascending=False).reset_index(drop=True)
print('RANKING GENERADO')

#generamos csv para subirlo a tableau en caso de que queramos tratar esos datos
final_df.to_csv('backups/ranking6w.csv',index=True,index_label='rank')
print('csv generado')

#creamos un df de los datos de los drivers para poder incluir sus datos en el mensaje y poder concatenar su correo electronico.
email_df = final_df.sort_values(by='Total_Score',ascending=False).reset_index(drop=True)
email_df = email_df.drop(['transport_type','score_WAPU','score_WADO','score_PACE PU Succeded','score_PACE DO Succeded','score_PACE PU vs DO','score_Online Rate','score_Cancelations','score_IAB'],axis=1)

#Eliminamos las columnas que no necesitamos para que el mensaje sea más facíl de hacer.
final_df = final_df.drop(['transport_type','WAPU','WADO','PACE PU Succeded','PACE DO Succeded','PACE PU vs DO','Online Rate','Cancelations','IAB'],axis=1)


# Calcular los valores mínimo y máximo de la columna 'score'
score_min = final_df['Total_Score'].min()
score_max = final_df['Total_Score'].max()

# Definir los porcentajes para los rangos(se puede cambiar, 
#...pero si queremos hacer más grupos habría que cambiar el códgio de la función)
porcentaje_superior = 0.9
porcentaje_medio = 0.7
porcentaje_bajo = 0.5

# Calcular los rangos en función de los porcentajes
rango_superior = score_min + (score_max - score_min) * porcentaje_superior
rango_medio = score_min + (score_max - score_min) * porcentaje_medio
rango_bajo = score_min + (score_max - score_min) * porcentaje_bajo

def enviar_mensaje(row):
    total_score = row['Total_Score']
    if not np.isnan(total_score):
        if total_score >= rango_superior:
            return "¡Excelente trabajo!"
        elif total_score >= rango_medio:
            return "Bien hecho."
        elif total_score >= rango_bajo:
            return "Sigue mejorando."
        else:
            # Obtener las columnas numéricas para encontrar el mínimo
            columnas_numericas = row.drop('Total_Score').loc[lambda x: pd.to_numeric(x, errors='coerce').notnull()]
            if not columnas_numericas.empty:
                columnas_trabajo = [key for key, value in columnas_numericas.items() if value == min(columnas_numericas.values)]
                mensajes = []
                # Ronda de IFs para que no quede feo el nombre de la columna
                if 'score_WAPU' in columnas_trabajo:
                    mensajes.append("¡El tiempo de espera en el local necesita mejorar!")
                if 'score_WADO' in columnas_trabajo:
                    mensajes.append("¡El cierre de los pedidos necesita mejorar!")
                if 'score_PACE PU Succeded' in columnas_trabajo:
                    mensajes.append("¡El ritmo de recogida necesita mejorar!")
                if 'score_PACE DO Succeded' in columnas_trabajo:
                    mensajes.append("¡El ritmo en la entrega necesita mejorar!")
                if 'score_PACE PU vs DO' in columnas_trabajo:
                    mensajes.append("Tardas más en ir a recoger que en entregar")
                if 'score_Online Rate' in columnas_trabajo:
                    mensajes.append("¡Tienes que asegurar estar conectado en tus horas!")
                if 'score_Cancelations' in columnas_trabajo:
                    mensajes.append("¡Recuerda que cancelar puede llevar a sanciones!")
                if 'score_IAB' in columnas_trabajo:
                    mensajes.append("¡Tienes que mejorar la puntualidad!")
                # En caso de que necesitemos más añadir aquí
                return " ".join(mensajes)
            else:
                return "No hay datos numéricos para comparar."
    else:
        return "No hay puntuación total disponible."

        
final_df['mensaje'] = final_df.apply(enviar_mensaje, axis=1)
print(f'Mensajes Generados')

dt = datetime.datetime.now()
print("Week number of the year : ", dt.strftime("%W"))

#Guardamos el csv con los scoring y su mensaje
final_df.to_csv(f'backups/Ranking_6W{dt.strftime("%W")}.csv')
print('csv generado')

#Guardamos un CSV para que veamos unicamente los mensajes enviados a cada driver en la semana actual, para poder consultarlo más adleante.
final_df['week'] =  dt.strftime("%W")
mensajes_enviados_df = final_df[['week','driver_id','mensaje']]
mensajes_enviados_df.to_csv(f'backups/Mensajes_W{dt.strftime("%W")}.csv')
print('csv generado')

#Mergeamos el correo que queremos envíar de sus datos, con el mensaje generado en función de sus métricas.
df_merged = pd.merge(email_df,final_df, on='driver_id', how='inner')
df_merged['email'] = 'bryanrarogal@gmail.com'

#Limpiamos los datos para que el correo se vea más bonito.
df_merged['WAPU'] = round(df_merged['WAPU'],2)
df_merged['WADO'] = round(df_merged['WADO'],2)
df_merged['PACE PU Succeded'] = round(df_merged['PACE PU Succeded'],2)
df_merged['PACE DO Succeded'] = round(df_merged['PACE DO Succeded'],2)
df_merged['PACE PU vs DO'] = round(df_merged['PACE PU vs DO'],2)
df_merged['Online Rate'] = (df_merged['Online Rate'] * 100).round(2).astype(str) + '%'
df_merged['Cancelations'] = (df_merged['Cancelations'] * 100).round(2).astype(str) + '%'
df_merged['IAB'] = (df_merged['IAB'] * 100).round(2).astype(str) + '%'

semana_anterior = dt.isocalendar()[1] - 1
df_semana_anterior = performance[performance['week'] == semana_anterior]  #performance[performance['week'] == 7]esto se usara para cuando el dataset este updated
print('df_semana_anterior_generado')

#Cremaos un nuevo df con las columnas que nos interesan.
df_semana_anterior_trans = df_semana_anterior.loc[:, columnas]

ad_columns = ['driver_id', 'transport_type']
t_columns = ad_columns + columnas_numericas
# Seleccionar las columnas del DataFrame df_semana_anterior
df_semana_anterior = df_semana_anterior.loc[:, t_columns]

print("DataFrame filtrado y seleccionado con columnas adicionales:")


#Normalizamos y sacamos nuevas columnas con el scooring de las columnas que hemos seleccionado
df_semana_anterior['score_WAPU'] = (df_semana_anterior['WAPU'] - df_semana_anterior['WAPU'].min()) / (df_semana_anterior['WAPU'].max() - df_semana_anterior['WAPU'].min())
df_semana_anterior['score_WADO'] = (df_semana_anterior['WADO'] - df_semana_anterior['WADO'].min()) / (df_semana_anterior['WADO'].max() - df_semana_anterior['WADO'].min())
df_semana_anterior['score_PACE PU Succeded'] = (df_semana_anterior['PACE PU Succeded'] - df_semana_anterior['PACE PU Succeded'].min()) / (df_semana_anterior['PACE PU Succeded'].max() - df_semana_anterior['PACE PU Succeded'].min())
df_semana_anterior['score_PACE DO Succeded'] = (df_semana_anterior['PACE DO Succeded'] - df_semana_anterior['PACE DO Succeded'].min()) / (df_semana_anterior['PACE DO Succeded'].max() - df_semana_anterior['PACE DO Succeded'].min())
df_semana_anterior['score_PACE PU vs DO'] = (df_semana_anterior['PACE PU vs DO'] - df_semana_anterior['PACE PU vs DO'].min()) / (df_semana_anterior['PACE PU vs DO'].max() - df_semana_anterior['PACE PU vs DO'].min())
df_semana_anterior['score_Online Rate'] = df_semana_anterior['Online Rate']
df_semana_anterior['score_Cancelations'] = df_semana_anterior['Cancelations']
df_semana_anterior['score_IAB'] = df_semana_anterior['IAB']

#Sacamos un único scooring sumando las positivas y restando las que són negativas.
df_semana_anterior['Total_Score'] = -df_semana_anterior['score_WAPU']-df_semana_anterior['score_WADO']-df_semana_anterior['score_PACE PU Succeded']-df_semana_anterior['score_PACE DO Succeded']-df_semana_anterior['score_PACE PU vs DO']+df_semana_anterior['score_Online Rate']-df_semana_anterior['score_Cancelations']+df_semana_anterior['score_IAB']

#Generamos el ranking para ver quienes son los tops.
df_semana_anterior = df_semana_anterior.sort_values(by='Total_Score',ascending=False).reset_index(drop=True)

#generamos csv para subirlo a tableau
df_semana_anterior.to_csv('backups/rankingw-1.csv',index=True,index_label='rank')
print('csv_generado')

#creamos un df de los datos de los drivers para poder incluir sus datos en el mensaje y poder concatenar su correo electronico.
email_df1w = df_semana_anterior.sort_values(by='Total_Score',ascending=False).reset_index(drop=True)
email_df1w = email_df1w.drop(['transport_type','score_WAPU','score_WADO','score_PACE PU Succeded','score_PACE DO Succeded','score_PACE PU vs DO','score_Online Rate','score_Cancelations','score_IAB'],axis=1)

#Eliminamos las columnas que no necesitamos para que el mensaje sea más facíl de hacer.
final_df2 = df_semana_anterior.drop(['transport_type','WAPU','WADO','PACE PU Succeded','PACE DO Succeded','PACE PU vs DO','Online Rate','Cancelations','IAB'],axis=1)
final_df2

# Calcular los valores mínimo y máximo de la columna 'score'
score_min = final_df2['Total_Score'].min()
score_max = final_df2['Total_Score'].max()

# Definir los porcentajes para los rangos(se puede cambiar, 
#...pero si queremos hacer más grupos habría que cambiar el códgio de la función)
porcentaje_superior = 0.9
porcentaje_medio = 0.7
porcentaje_bajo = 0.5

# Calcular los rangos en función de los porcentajes
rango_superior = score_min + (score_max - score_min) * porcentaje_superior
rango_medio = score_min + (score_max - score_min) * porcentaje_medio
rango_bajo = score_min + (score_max - score_min) * porcentaje_bajo
        
final_df2['mensaje'] = final_df2.apply(enviar_mensaje, axis=1)
print(f'Mensajes Generados')

#Guardamos el csv con los scoring y su mensaje
final_df2.to_csv(f'backups/Ranking_1W{dt.strftime("%W")}.csv')
print('csv generado')

#Guardamos un CSV para que veamos unicamente los mensajes enviados a cada driver en la semana actual, para poder consultarlo más adleante.
final_df2['week'] =  dt.strftime("%W")
mensajes_enviados_df2 = final_df2[['week','driver_id','mensaje']]
mensajes_enviados_df2.to_csv(f'backups/Mensajes_W{dt.strftime("%W")}.csv')
print('csv generado')

#Mergeamos el correo que queremos envíar de sus datos, con el mensaje generado en función de sus métricas.
df_merged2 = pd.merge(email_df1w,final_df2, on='driver_id', how='inner')
df_merged2['email'] = 'bryanrarogal@gmail.com'

#Limpiamos los datos para que el correo se vea más bonito.
df_merged2['WAPU'] = round(df_merged2['WAPU'],2)
df_merged2['WADO'] = round(df_merged2['WADO'],2)
df_merged2['PACE PU Succeded'] = round(df_merged2['PACE PU Succeded'],2)
df_merged2['PACE DO Succeded'] = round(df_merged2['PACE DO Succeded'],2)
df_merged2['PACE PU vs DO'] = round(df_merged2['PACE PU vs DO'],2)
df_merged2['Online Rate'] = (df_merged2['Online Rate'] * 100).round(2).astype(str) + '%'
df_merged2['Cancelations'] = (df_merged2['Cancelations'] * 100).round(2).astype(str) + '%'
df_merged2['IAB'] = (df_merged2['IAB'] * 100).round(2).astype(str) + '%'

#Este será uno de los correos de los datos que usaremos para enviar su correo.
df_merged2.to_csv("backups/dataw-1.csv")
print('csv generado')

test6w = df_merged.drop(['week','mensaje'],axis=1)

test1w = df_merged2.drop(['week'],axis=1)

merged_test = pd.merge(test1w,test6w, on ='driver_id',how='left')
print('dataframe final generado y listo para envío')

#Cambiamos algunos correos para que los compis lo vean
merged_test.at[0,'email_y'] = dotenv_values('.env')['email_1']
merged_test.at[1,'email_y'] = dotenv_values('.env')['email_2']
merged_test.at[2,'email_y'] = dotenv_values('.env')['email_3']
merged_test.at[3,'email_y'] = dotenv_values('.env')['email_4']
merged_test.at[4,'email_y'] = dotenv_values('.env')['email_5']
merged_test.at[5,'email_y'] = dotenv_values('.env')['email_6']
merged_test.at[6,'email_y'] = dotenv_values('.env')['email_7']
merged_test.at[7,'email_y'] = dotenv_values('.env')['email_8']
merged_test.at[8,'email_y'] = dotenv_values('.env')['email_9']
merged_test.at[9,'email_y'] = dotenv_values('.env')['email_10']
merged_test.at[10,'email_y'] = dotenv_values('.env')['email_11']
merged_test.at[11,'email_y'] = dotenv_values('.env')['email_12']
merged_test.at[12,'email_y'] = dotenv_values('.env')['email_13']
merged_test.at[13,'email_y'] = dotenv_values('.env')['email_14']
merged_test.at[14,'email_y'] = dotenv_values('.env')['email_15']

print('correos de clase cargados')

email_emisor = 'jbryanrd@gmail.com'
email_contraseña = dotenv_values('.env')['PWDG']
email_receptor = 'bryanrarogal@gmail.com'

# Función para enviar correos electrónicos
def enviar_correo(destinatario, asunto, cuerpo):
    # Configurar el correo electrónico
    email_emisor = 'jbryanrd@gmail.com'
    email_contraseña = dotenv_values('.env')['PWDG']

    # Crear el mensaje
    mensaje = MIMEMultipart('alternative')
    mensaje['Subject'] = asunto
    mensaje['From'] = email_emisor
    mensaje['To'] = destinatario

    # Adjuntar la parte HTML al mensaje
    parte_html = MIMEText(cuerpo, 'html')
    mensaje.attach(parte_html)

    # Enviar el correo electrónico
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.send_message(mensaje)

    print("Correo electrónico enviado exitosamente.")

for index, row in merged_test.iterrows():
    driver_id = row['driver_id']
    destinatario = row['email_x']
    mensaje = row['mensaje']

    # Obtenemos los datos de la semana actual
    semana_actual_data = {
        'Minutos en el punto de recogida': row['WAPU_x'],
        'Minutos en el punto de entrega': row['WADO_x'],
        'Ritmo de recogida (m/km)': row['PACE PU Succeded_x'],
        'Ritmo de entrega (m/km)': row['PACE DO Succeded_x'],
        'Diferencia entre tus ritmos': row['PACE PU vs DO_x'],
        'Porcentaje de conexión': row['Online Rate_x'],
        'Porcentaje de pedidos cancelados': row['Cancelations_x'],
        'Porcentaje de puntualidad': row['IAB_x']
    }

    # Obtenemos los datos de la semana anterior
    semana_anterior_data = {
        'Minutos en el punto de recogida': row['WAPU_y'],
        'Minutos en el punto de entrega': row['WADO_y'],
        'Ritmo de recogida (m/km)': row['PACE PU Succeded_y'],
        'Ritmo de entrega (m/km)': row['PACE DO Succeded_y'],
        'Diferencia entre tus ritmos': row['PACE PU vs DO_y'],
        'Porcentaje de conexión': row['Online Rate_y'],
        'Porcentaje de pedidos cancelados': row['Cancelations_y'],
        'Porcentaje de puntualidad': row['IAB_y']
    }
    
    # Creamos un DataFrame para las métricas de ambas semanas
    df_metrics = pd.DataFrame({
        'Métrica': list(semana_actual_data.keys()),
        'Semana Actual': list(semana_actual_data.values()),
        'Semana Anterior': list(semana_anterior_data.values())
    })
    
    # Cuerpo del correo electrónico en HTML
    cuerpo_correo_html = f"""
    <html>
    <head>
      <style>
        body {{
          font-family: Arial, sans-serif;
          color: #333;
        }}
        .titulo {{
          font-size: 18px;
          font-weight: bold;
          margin-bottom: 5px;
        }}
        .valor {{
          font-size: 16px;
          color: #666;
        }}
        .mensaje {{
          margin-top: 15px;
          font-style: italic;
        }}
        table {{
          border-collapse: collapse;
          width: 100%;
        }}
        th, td {{
          padding: 8px;
          text-align: left;
          border-bottom: 1px solid #ddd;
        }}
        th {{
          background-color: #f2f2f2;
        }}
      </style>
    </head>
    <body>
      <p>👋Hola👋,</p>
      <p>🚀Te enviamos tus métricas🚀:</p>
     
      <table>
        <tr>
          <th>Métrica</th>
          <th>Semana Anterior</th>
          <th>Ultimas 6 semanas</th>
        </tr>
    """

    # Agregar filas de la tabla con las métricas de ambas semanas
    for _, metric_row in df_metrics.iterrows():
        cuerpo_correo_html += f"""
        <tr>
          <td>{metric_row['Métrica']}</td>
          <td>{metric_row['Semana Actual']}</td>
          <td>{metric_row['Semana Anterior']}</td>
        </tr>
        """

    # Cerrar el cuerpo del correo electrónico HTML
    cuerpo_correo_html += f"""
      </table>
      <p class="mensaje" style="font-size: 24px;">{mensaje} </p>
      <p>¡Gracias!</p>
    </body>
    </html>
    """

    # Enviar correo electrónico
    enviar_correo(destinatario, 'Aquí tienes tus métricas', cuerpo_correo_html)

print("Todos los correos electrónicos enviados exitosamente.")
