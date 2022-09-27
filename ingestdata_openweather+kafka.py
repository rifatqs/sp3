from re import X
import requests,json
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

def cekkondisi(inikota):
    website = requests.get("https://api.openweathermap.org/data/2.5/weather?q="+inikota+"&appid=7b6c6cc78e0752ceded4bb7b6e4453ef")
    #website = requests.get("https://api.openweathermap.org/data/2.5/weather?q=bali&appid=7b6c6cc78e0752ceded4bb7b6e4453ef")
    cuaca = website.json()

    dt = datetime.now()
    ts = datetime.timestamp(dt)
    st = datetime.fromtimestamp(ts)
    waktu = st.isoformat(timespec='seconds')

    kondisi_cuaca = cuaca['weather'][0]['main']
    kondisi_cuaca_desc = cuaca['weather'][0]['description']
    kondisi_suhu = cuaca['main']['temp']
    kondisi_suhu_min = cuaca['main']['temp_min']
    kondisi_suhu_max = cuaca['main']['temp_max']

    output = {}
    output['kota'] = inikota
    output['cuaca'] = kondisi_cuaca
    output['cuaca desc'] = kondisi_cuaca_desc
    output['suhu'] = kondisi_suhu
    output['suhu_min'] = kondisi_suhu_min
    output['suhu max'] = kondisi_suhu_max
    output['waktu'] = waktu

   # print(output)
    return output

def cetak_kota():
    daftar_kota = ['bali','lombok','labuan bajo','medan','bandung']
    for x in daftar_kota:
    # print('kota '+x)
        data = {'counter': x}
        producer.send('topic_weather', value=data)
        print(cekkondisi(x))

cetak_kota()

# for j in range(9999):
#     print("Iteration", j)

#     sleep(0.5)