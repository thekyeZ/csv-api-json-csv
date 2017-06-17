#!/usr/bin/env python3
import csv
import urllib2
from urllib2 import urlopen
import json


#tutaj ustawic sciezke do wynikowego pliku CSV
result_file = "final.csv"

#tutaj ustawic sciezke do pliku z uuid (poprzednia sciezka: '/home/abc/Documents/opentests1.csv')
uuid_file = "/Users/kyez/Desktop/api-json-csv/opentests1_ori.csv"

#tutaj ustawic sciezke do pliku z pobranymi z API JSON'ami
#UWAGA! Przy wlaczaniu skryptu ten plik musi NIE istniec
jsons_file = "/Users/kyez/Desktop/api-json-csv/JSONOBJECTS.json"

#sprawdza ile obiektow json zostalo wczytanych
json_counter = 0

final_data = ""

#fukcja opowiedzialna za wpisywanie do komorek opowiednich wartosci
def func(item, int, rest):
    try:
        return item[int][rest]
    except:
        return ""


#odczyt danych z url oraz zapisywanie do jednej zmiennej wszystkich wynikow
print "Reciveing data in progress..."

with open(uuid_file, 'r') as f:
 reader = csv.reader(f)
 my_list = list(reader)


for x in my_list:
        y = str(x)
        y0 = y.replace(";;", "")
        y1 = y0.replace("['", "")
        y2 = y1.replace("']", "")

        url = 'https://www.netztest.at/opendata/opentests/'
        full_url = url + y2

        if y2 != '' :
            data = urllib2.urlopen(full_url).read()
            json_counter = json_counter + 1

            if (json_counter == 1):
                final_data += "["

            final_data += data

            if (json_counter != (len(my_list))):
                final_data += ",\n"

            if (json_counter == (len(my_list))):
                final_data += "]"

        print "%d of all %d done" % (json_counter, (len(my_list)))


#zapis pobranych danych do pliku
file = open(jsons_file, 'a')

file.write(final_data.decode('ascii', 'ignore'))

file.close()


# json to data parsing
print "Data fully recived and saved. JSON to CSV processing"

with open(jsons_file, 'r') as f:

    json_data = json.load(f)

    output_file = csv.writer(open(result_file, "wb+"))
    output_file.writerow(["upload_classification", "test_duration", "testdl_if_bytes_upload", "sim_country",	"ndt_download_kbit", "zip_code", "time_ul_ms",	"province",	"time_dl_ms", "public_ip_as_name", "gkz", "country_geoip", "ping_classification", "roaming_type",	"implausible",	"model",	"connection",	"signal_strength",	"lat",	"speed_curve__download__time_elapsed",	"speed_curve__download__bytes_total",	"speed_curve__upload__time_elapsed",	"speed_curve__upload__bytes_total",	"speed_curve__signal__time_elapsed",	"speed_curve__signal__lte_rsrq",	"speed_curve__signal__lte_rsrp",	"speed_curve__signal__cat_technology",	"speed_curve__signal__signal_strength",	"speed_curve__signal__network_type",	"test_if_bytes_upload",	"duration_download_ms",	"network_name",	"bytes_upload",	"cov800cat",	"community", 	"signal_classification",	"ip_anonym",	"upload_kbit",	"ping_ms",	"district",	"wifi_link_speed",	"num_threads",	"cat_technology","open_uuid",	"server_name",	"pinned", "model_native",	"distance",	"network_mcc_mnc",	"country_location",	"download_classification",	"long",	"platform",	"testul_if_bytes_download",	"loc_src",	"loc_accuracy",	"bytes_download",	"open_test_uuid",	"country_asn",	"ndt_upload_kbit",	"num_threads_ul", "client_version",	"provider_name",	"network_country",	"product",	"duration_upload_ms",	"testul_if_bytes_upload",	"num_threads_requested",	"sim_mcc_mnc",	"test_if_bytes_download",	"lte_rsrq",	"lte_rsrp",	"time",	"download_kbit",	"network_type",	"asn",	"testdl_if_bytes_download"])

    for j in json_data: #all json objets in global tab

        #write first row
        output_file.writerow([j["upload_classification"], j["test_duration"], j["testdl_if_bytes_upload"], j["sim_country"], j["ndt_download_kbit"], j["zip_code"], j["time_ul_ms"], j["province"],	j["time_dl_ms"], j["public_ip_as_name"], j["gkz"], j["country_geoip"], j["ping_classification"], j["roaming_type"],	j["implausible"],	j["model"],	j["connection"],	j["signal_strength"],	j["lat"],	j['speed_curve']['download'][0]['time_elapsed'],	j['speed_curve']['download'][0]['bytes_total'],	j['speed_curve']['upload'][0]['time_elapsed'], j['speed_curve']['upload'][0]['bytes_total'], j['speed_curve']['signal'][0]['time_elapsed'], j['speed_curve']['signal'][0]['lte_rsrq'], j['speed_curve']['signal'][0]['lte_rsrp'], j['speed_curve']['signal'][0]['cat_technology'], j['speed_curve']['signal'][0]['signal_strength'], j['speed_curve']['signal'][0]['network_type'],	j["test_if_bytes_upload"],	j["duration_download_ms"],	j["network_name"],	j["bytes_upload"],	j["cov800cat"],	j["community"], 	j["signal_classification"],	j["ip_anonym"],	j["upload_kbit"],	j["ping_ms"],	j["district"],	j["wifi_link_speed"],	j["num_threads"],	j["cat_technology"], j["open_uuid"],	j["server_name"],	j["pinned"],	j["model_native"],	j["distance"],	j["network_mcc_mnc"],	j["country_location"],	j["download_classification"],	j["long"],	j["platform"],	j["testul_if_bytes_download"],	j["loc_src"],	j["loc_accuracy"],	j["bytes_download"],	j["open_test_uuid"],	j["country_asn"],	j["ndt_upload_kbit"],	j["num_threads_ul"],	j["client_version"],	j["provider_name"],	j["network_country"],	j["product"],	j["duration_upload_ms"],	j["testul_if_bytes_upload"],	j["num_threads_requested"],	j["sim_mcc_mnc"],	j["test_if_bytes_download"],	j["lte_rsrq"],	j["lte_rsrp"],	j["time"],	j["download_kbit"],	j["network_type"],	j["asn"],	j["testdl_if_bytes_download"] ] )

        donwnload_len = len(j['speed_curve']['download'])
        upload_len = len(j['speed_curve']['upload'])
        signal_len = len(j['speed_curve']['signal'])

        max_elems = max(donwnload_len, upload_len, signal_len)

        for i in range(1, max_elems) :
            output_file.writerow(
                ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                 func(j['speed_curve']['download'], i, 'time_elapsed'),
                 func(j['speed_curve']['download'], i, 'bytes_total'),
                 func(j['speed_curve']['upload'],i, 'time_elapsed'),
                 func(j['speed_curve']['upload'], i, 'bytes_total'),
                 func(j['speed_curve']['signal'], i, 'time_elapsed'),
                 func(j['speed_curve']['signal'], i, 'lte_rsrq'),
                 func(j['speed_curve']['signal'], i, 'lte_rsrp'),
                 func(j['speed_curve']['signal'], i, 'cat_technology'),
                 func(j['speed_curve']['signal'], i, 'signal_strength'),
                 func(j['speed_curve']['signal'], i, 'network_type'),
                 "", "", "","", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""])


print "Job done. CSV file saved! File name: %s" % result_file