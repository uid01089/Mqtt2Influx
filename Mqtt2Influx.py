import logging
import time
import re
from enum import Enum, auto


import paho.mqtt.client as pahoMqtt
from PythonLib.JsonUtil import JsonUtil
from PythonLib.Mqtt import MQTTHandler, Mqtt
from PythonLib.Scheduler import Scheduler
from PythonLib.DateUtil import DateTimeUtilities
from PythonLib.StringUtil import StringUtil
from PythonLib.Influx import Influx

logger = logging.getLogger('Mqtt2Influx')


'''
    Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported
    and mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
    respectively. For infinite retention, meaning the data will
    never be deleted, use 'INF' for duration.
    The minimum retention period is 1 hour.
'''



class Module:
    def __init__(self) -> None:
        self.scheduler = Scheduler()
        self.influxDb = Influx('koserver.parents', database="myhome")
        self.mqttClient = Mqtt("koserver.iot", "/house", pahoMqtt.Client("Mqtt2Influx1", protocol=pahoMqtt.MQTTv311))

    def getScheduler(self) -> Scheduler:
        return self.scheduler

    def getInfluxDb(self) -> Influx:
        return self.influxDb

    def getMqttClient(self) -> Mqtt:
        return self.mqttClient

    def setup(self) -> None:
        self.scheduler.scheduleEach(self.mqttClient.loop, 500)

    def loop(self) -> None:
        self.scheduler.loop()


class Mqtt2Influx:

    def __init__(self, module: Module) -> None:
        self.mqttClient = module.getMqttClient()
        self.scheduler = module.getScheduler()
        self.influxDb = module.getInfluxDb()

        self.includePattern = []

    def setup(self) -> None:

        self.mqttClient.subscribeStartWithTopic("/house/", self.receiveData)
        self.scheduler.scheduleEach(self.__keepAlive, 10000)
        # self.influxDb.deleteDatabase()
        # self.influxDb.createDatabase()

        # self.includePattern.append((re.compile('/house/agents/FroelingP2/heartbeat'),"INF"))

        self.includePattern.append((re.compile('/house/garden/automower/mower/status$'),"1w"))
        self.includePattern.append((re.compile('/house/garden/automower/mower/status/plain'),"1w"))
        self.includePattern.append((re.compile('/house/garden/automower/mower/battery/charge'),"1w"))
        self.includePattern.append((re.compile('/house/garden/automower/mower/error/message'),"1w"))
        self.includePattern.append((re.compile('/house/garden/automower/health/voltage/batt'),"1w"))
        self.includePattern.append((re.compile('/house/garden/automower/mower/distance'),"1w"))
        self.includePattern.append((re.compile('/house/garden/automower/mower/mode'),"1w"))

        self.includePattern.append((re.compile('/house/basement/solartherm/measurement/TempSensor2'),"1w"))
        self.includePattern.append((re.compile('/house/basement/solartherm/measurement/TempSensor1'),"1w"))
        self.includePattern.append((re.compile('/house/basement/solartherm/measurement/TempSensor4'),"1w"))
        self.includePattern.append((re.compile('/house/basement/solartherm/measurement/OperationHoursRelay1'),"1w"))
        self.includePattern.append((re.compile('/house/basement/solartherm/measurement/HeatQuantity'),"1w"))
        self.includePattern.append((re.compile('/house/basement/solartherm/measurement/PumpSpeedRelay1'),"1w"))

        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Puffert.un'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Puffert.ob'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Kesseltemp'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Betriebszustand'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Fuellst.:'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Einschub'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Außentemp'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Vorlauft.2'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Vorlauft.1'),"1w"))
        self.includePattern.append((re.compile('/house/basement/heizung/measurement/Brenn.ST'),"1w"))

        self.includePattern.append((re.compile('/house/attic/vallox/measurement/IncomingTemp'),"1w"))
        self.includePattern.append((re.compile('/house/attic/vallox/measurement/InsideTemp'),"1w"))
        self.includePattern.append((re.compile('/house/attic/vallox/measurement/OutsideTemp'),"1w"))
        self.includePattern.append((re.compile('/house/attic/vallox/measurement/ExhaustTemp'),"1w"))
        self.includePattern.append((re.compile('/house/attic/vallox/measurement/FanSpeed'),"1w"))
        self.includePattern.append((re.compile('/house/attic/vallox/measurement/PowerState'),"1w"))
        self.includePattern.append((re.compile('/house/attic/vallox/measurement/FilterguardIndicator'),"1w"))

        self.includePattern.append((re.compile('/house/garden/weatherstation/temp_c'),"1w"))
        self.includePattern.append((re.compile('/house/garden/weatherstation/humidity'),"1w"))
        self.includePattern.append((re.compile('/house/garden/weatherstation/wind_gust_meter_sec'),"1w"))
        self.includePattern.append((re.compile('/house/garden/weatherstation/wind_avg_meter_sec'),"1w"))
        self.includePattern.append((re.compile('/house/garden/weatherstation/wind_direction_deg'),"1w"))
        self.includePattern.append((re.compile('/house/garden/weatherstation/rain_mm'),"1w"))

        self.includePattern.append((re.compile('/house/rooms/.+?/Temperature'),"1w"))
        self.includePattern.append((re.compile('/house/rooms/.+?/Humidity'),"1w"))
        self.includePattern.append((re.compile('/house/rooms/.+?/TargetTemperature'),"1w"))
        self.includePattern.append((re.compile('/house/rooms/.+?/heating'),"1w"))

        self.includePattern.append((re.compile('/house/basement/ess/essinfo_common/BATT/soc'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_common/LOAD/load_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_common/GRID/active_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_common/BATT/dc_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_common/PV/pv1_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_common/PV/pv2_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_common/PV/pv3_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_home/direction/.+?'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_home/statistics/pcs_pv_total_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_home/statistics/batconv_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_home/statistics/load_power'),"INF"))
        self.includePattern.append((re.compile('/house/basement/ess/essinfo_home/statistics/grid_power'),"INF"))

        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewUpstreamAttenuation'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewDownstreamAttenuation'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewUpstreamNoiseMargin'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewDownstreamNoiseMargin'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewUpstreamCurrRate'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewUpstreamMaxRate'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewDownstreamCurrRate'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewDownstreamMaxRate'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewFECErrors'),"4w"))
        self.includePattern.append((re.compile('/house/groundfloor/fritz/GetDSLInfo/NewCRCErrors'),"4w"))

        self.includePattern.append((re.compile('/house/.+?/mikrotik/.*?/cpu-load'),"2d"))
        self.includePattern.append((re.compile('/house/.+?/mikrotik/.*?/traffic/.+?/rx-bits-per-second'),"2d"))
        self.includePattern.append((re.compile('/house/.+?/mikrotik/.*?/traffic/.+?/tx-bits-per-second'),"2d"))

        self.includePattern.append((re.compile('/house/agents/ChargeControl/data/.*'),"1w"))

    def receiveData(self, topic: str, payloadStr: str) -> None:

        try:

            matched = False
            payload = None

            for pattern_retention in self.includePattern:
                pattern = pattern_retention[0]
                retention = pattern_retention[1]

                if pattern.match(topic):
                    matched = True
                    break

            if matched:

                # Check if the variable contains a string
                if StringUtil.isNubmer(payloadStr.strip()):
                    payload = float(payloadStr.strip())
                elif (isBool := StringUtil.isBoolean(payloadStr.strip())) is not None:
                    payload = 1 if isBool else 0
                else:
                    payload = payloadStr

                # print(topic, payload)
                self.influxDb.writeOnChange('Mqtt'+"_"+retention, {topic: payload},60000,retention)

        except BaseException:
            logging.exception('_1_')

    def __keepAlive(self) -> None:
        self.mqttClient.publishIndependentTopic('/house/agents/Mqtt2Influx/heartbeat', DateTimeUtilities.getCurrentDateString())
        self.mqttClient.publishIndependentTopic('/house/agents/Mqtt2Influx/subscriptions', JsonUtil.obj2Json(self.mqttClient.getSubscriptionCatalog()))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('Mqtt2Influx').setLevel(logging.DEBUG)
    logging.getLogger('PythonLib.Mqtt').setLevel(logging.INFO)

    module = Module()
    module.setup()

    logging.getLogger('Mqtt2Influx').addHandler(MQTTHandler(module.getMqttClient(), '/house/agents/Mqtt2Influx/log'),"INF"))

    Mqtt2Influx(module).setup()

    print("Mqtt2Influx is running")

    while (True):
        module.loop()
        time.sleep(0.25)


if __name__ == '__main__':
    main()
